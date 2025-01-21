/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.utils.Pool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import scala.collection._
import scala.jdk.CollectionConverters._

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  // 标识是否正在等待 ISR 集合中的 follower 副本从 leader 副本同步 requiredOffset 之前的消息
  @volatile var acksPending = false

  override def toString: String = s"[acksPending: $acksPending, error: ${responseStatus.error.code}, " +
    s"startOffset: ${responseStatus.baseOffset}, requiredOffset: $requiredOffset]"
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short,// 对应 acks 值设置
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) { // 记录每个 topic 分区对应的消息追加状态

  override def toString = s"[requiredAcks: $produceRequiredAcks, partitionStatus: $produceStatus]"
}

object DelayedProduce {
  private final val logger = Logger(classOf[DelayedProduce])
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
 */
class DelayedProduce(delayMs: Long,// 延迟时长
                     produceMetadata: ProduceMetadata,// 用于判断 DelayedProduce 是否满足执行条件
                     replicaManager: ReplicaManager,// 副本管理器
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit,// 回调函数，在任务满足条件或到期时执行
                     lockOpt: Option[Lock] = None)
  extends DelayedOperation(delayMs, lockOpt) {

  override lazy val logger: Logger = DelayedProduce.logger

  // first update the acks pending variable according to the error code
  // 依据消息写入 leader 分区操作的错误码对 produceMetadata 的 produceStatus 进行初始化
  produceMetadata.produceStatus.foreachEntry { (topicPartition, status) =>
    if (status.responseStatus.error == Errors.NONE) {
      // Timeout error state will be cleared when required acks are received
      // 对应 topic 分区消息写入 leader 副本成功，等待其它副本同步
      status.acksPending = true
      // 默认错误码
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      // 对应 topic 分区消息写入 leader 副本失败，无需等待
      status.acksPending = false
    }

    trace(s"Initial partition status for $topicPartition is $status")
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: Replica not assigned to partition
   * Case B: Replica is no longer the leader of this partition 对应 topic 分区的 leader 副本不再位于当前 broker 节点上
   * Case C: This broker is the leader:
   *   C.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response 检查 ISR 集合中的所有 follower 副本是否完成同步时出现异常
   *   C.2 - Otherwise, set the response with no error. ISR 集合中所有的 follower 副本完成了同步操作
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    // 遍历处理所有的 topic 分区
    produceMetadata.produceStatus.foreachEntry { (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // skip those partitions that have already been satisfied
      // 仅处理正在等待 follower 副本复制的分区
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartitionOrError(topicPartition) match {
          // 错误
          case Left(err) =>
            // Case A
            (false, err)

            // 成功
          case Right(partition) =>
            // 检测对应分区本次追加的最后一条消息是否已经被 ISR 集合中所有的 follower 副本同步
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
        }

        // Case B || C.1 || C.2
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }

    // check if every partition has satisfied at least one of case A, B or C
    // 如果所有的 topic 分区都已经满足了 DelayedProduce 的执行条件，即不存在等待 ack 的分区，则结束本次延时任务
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    produceMetadata.produceStatus.foreachEntry { (topicPartition, status) =>
      if (status.acksPending) {
        debug(s"Expiring produce request for partition $topicPartition with status $status")
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete(): Unit = {
    val responseStatus = produceMetadata.produceStatus.map { case (k, status) => k -> status.responseStatus }
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics {
  private val metricsGroup = new KafkaMetricsGroup(DelayedProduceMetrics.getClass)

  private val aggregateExpirationMeter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    metricsGroup.newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             Map("topic" -> key.topic, "partition" -> key.partition.toString).asJava)
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition): Unit = {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}
