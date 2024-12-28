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
package kafka.controller

import com.yammer.metrics.core.{Gauge, Timer}
import kafka.cluster.Broker
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients._
import org.apache.kafka.common._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.ShutdownableThread

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.collection.{Seq, Set, mutable}
import scala.jdk.CollectionConverters._

object ControllerChannelManager {
  private val QueueSizeMetricName = "QueueSize"
  private val RequestRateAndQueueTimeMetricName = "RequestRateAndQueueTimeMs"
}

// Controller向Broker发送请求所用的通道管理器
// 管理 Controller 与集群 Broker 之间的连接，并为每个 Broker 创建 RequestSendThread 线程实例；
// 将要发送的请求放入到指定 Broker 的阻塞队列中，等待该 Broker 专属的 RequestSendThread 线程进行处理。
class ControllerChannelManager(controllerEpoch: () => Int,
                               config: KafkaConfig,
                               time: Time,
                               metrics: Metrics,
                               stateChangeLogger: StateChangeLogger,
                               threadNamePrefix: Option[String] = None) extends Logging {
  import ControllerChannelManager._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  protected val brokerStateInfo = new mutable.HashMap[Int/*集群中Broker的ID信息*/, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  metricsGroup.newGauge("TotalQueueSize",
    () => brokerLock synchronized {
      brokerStateInfo.values.iterator.map(_.messageQueue.size).sum
    }
  )

  /**
   * Controller 组件在启动时，会调用 ControllerChannelManager 的 startup 方法。
   * 该方法会从元数据信息中找到集群的 Broker 列表，然后依次为它们调用 addBroker 方法，把它们加到 brokerStateInfo 变量中，
   * 最后再依次启动 brokerStateInfo 中的 RequestSendThread 线程。
   * @param initialBrokers
   */
  def startup(initialBrokers: Set[Broker]):Unit = {
    initialBrokers.foreach(addNewBroker)

    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  /**
   * 关闭所有 RequestSendThread 线程，并清空必要的资源
   */
  def shutdown():Unit = {
    brokerLock synchronized {
      brokerStateInfo.values.toList.foreach(removeExistingBroker)
    }
  }

  /**
   * 从名字看，就是发送请求，实际上就是把请求对象提交到请求队列。
   * @param brokerId
   * @param request
   * @param callback
   */
  def sendRequest(brokerId: Int, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(request.apiKey, request, callback, time.milliseconds()))
        case None =>
          warn(s"Not sending request ${request.apiKey.name} with controllerId=${request.controllerId()}, " +
            s"controllerEpoch=${request.controllerEpoch()}, brokerEpoch=${request.brokerEpoch()} " +
            s"to broker $brokerId, since it is offline.")
      }
    }
  }

  /**
   * 添加目标 Broker 到 brokerStateInfo 数据结构中，并创建必要的配套资源，如请求队列、RequestSendThread 线程对象等。
   * 最后，RequestSendThread 启动线程
   * @param broker
   */
  def addBroker(broker: Broker): Unit = {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      // 如果该Broker是新Broker的话
      if (!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  /**
   * 从 brokerStateInfo 移除目标 Broker 的相关数据
   * @param brokerId
   */
  def removeBroker(brokerId: Int): Unit = {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  private def addNewBroker(broker: Broker): Unit = {
    // 为该Broker构造请求阻塞队列
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug(s"Controller ${config.brokerId} trying to connect to broker ${broker.id}")
    val controllerToBrokerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val controllerToBrokerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
    // 获取待连接Broker节点对象信息
    val brokerNode = broker.node(controllerToBrokerListenerName)
    val logContext = new LogContext(s"[Controller id=${config.brokerId}, targetBrokerId=${brokerNode.idString}] ")
    val (networkClient, reconfigurableChannelBuilder) = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerToBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerToBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      val reconfigurableChannelBuilder = channelBuilder match {
        case reconfigurable: Reconfigurable =>
          config.addReconfigurable(reconfigurable)
          Some(reconfigurable)
        case _ => None
      }
      // 创建NIO Selector实例用于网络数据传输
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> brokerNode.idString).asJava,
        false,
        channelBuilder,
        logContext
      )
      // 创建NetworkClient实例
      // NetworkClient类是Kafka clients工程封装的顶层网络客户端API
      // 提供了丰富的方法实现网络层IO数据传输
      val networkClient = new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        config.connectionSetupTimeoutMs,
        config.connectionSetupTimeoutMaxMs,
        time,
        false,
        new ApiVersions,
        logContext,
        MetadataRecoveryStrategy.NONE
      )
      (networkClient, reconfigurableChannelBuilder)
    }
    // 为这个RequestSendThread线程设置线程名称
    val threadName = threadNamePrefix match {
      case None => s"Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
      case Some(name) => s"$name:Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
    }

    // 构造请求处理速率监控指标
    val requestRateAndQueueTimeMetrics = metricsGroup.newTimer(
      RequestRateAndQueueTimeMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS, brokerMetricTags(broker.id)
    )

    // 创建RequestSendThread实例
    val requestThread = new RequestSendThread(config.brokerId, controllerEpoch, messageQueue, networkClient,
      brokerNode, config, time, requestRateAndQueueTimeMetrics, stateChangeLogger, threadName)
    requestThread.setDaemon(false)

    val queueSizeGauge = metricsGroup.newGauge(QueueSizeMetricName, () => messageQueue.size, brokerMetricTags(broker.id))

    // 创建该Broker专属的ControllerBrokerStateInfo实例
    // 并将其加入到brokerStateInfo统一管理
    brokerStateInfo.put(broker.id, ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge, requestRateAndQueueTimeMetrics, reconfigurableChannelBuilder))
  }

  private def brokerMetricTags(brokerId: Int) = Map("broker-id" -> brokerId.toString).asJava

  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo): Unit = {
    try {
      // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the
      // non-threadsafe classes as described in KAFKA-4959.
      // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that
      // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.
      brokerState.reconfigurableChannelBuilder.foreach(config.removeReconfigurable)
      brokerState.requestSendThread.shutdown()
      brokerState.networkClient.close()
      brokerState.messageQueue.clear()
      metricsGroup.removeMetric(QueueSizeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      metricsGroup.removeMetric(RequestRateAndQueueTimeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  private def startRequestSendThread(brokerId: Int): Unit = {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if (requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

case class QueueItem(apiKey: ApiKeys, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                     callback: AbstractResponse => Unit, enqueueTimeMs: Long)

// Controller 会为集群中的每个 Broker 都创建一个对应的 RequestSendThread 线程。
// Broker 上的这个线程，持续地从阻塞队列中获取待发送的请求。
class RequestSendThread(val controllerId: Int, // Controller所在Broker的Id
                        controllerEpoch: () => Int,
                        val queue: BlockingQueue[QueueItem], // 请求阻塞队列
                        val networkClient: NetworkClient,// 用于执行发送的网络I/O类
                        val brokerNode: Node,// 目标Broker节点
                        val config: KafkaConfig,// Kafka配置信息
                        val time: Time,
                        val requestRateAndQueueTimeMetrics: Timer,
                        val stateChangeLogger: StateChangeLogger,
                        name: String)
  extends ShutdownableThread(name, true, s"[RequestSendThread controllerId=$controllerId] ")
    with Logging {

  logIdent = logPrefix

  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  override def doWork(): Unit = {

    def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

    val QueueItem(apiKey, requestBuilder, callback, enqueueTimeMs) = queue.take()
    requestRateAndQueueTimeMetrics.update(time.milliseconds() - enqueueTimeMs, TimeUnit.MILLISECONDS)

    var clientResponse: ClientResponse = null
    try {
      var isSendSuccessful = false
      while (isRunning && !isSendSuccessful) {
        // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
        // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
        try {
          // 如果没有创建与目标Broker的TCP连接，或连接暂时不可用
          if (!brokerReady()) {
            isSendSuccessful = false
            // 等待重试
            backoff()
          }
          else {
            val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder,
              time.milliseconds(), true)
            // 发送请求，等待接收Response （会同步阻塞）
            clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
            isSendSuccessful = true
          }
        } catch {
          case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
            warn(s"Controller $controllerId epoch ${controllerEpoch()} fails to send request " +
              s"$requestBuilder " +
              s"to broker $brokerNode. Reconnecting to broker.", e)
            // 如果出现异常，关闭与对应Broker的连接
            networkClient.close(brokerNode.idString)
            isSendSuccessful = false
            backoff()
        }
      }
      // 如果接收到了Response
      if (clientResponse != null) {
        val requestHeader = clientResponse.requestHeader
        val api = requestHeader.apiKey
        // 此Response的请求类型必须是LeaderAndIsrRequest、StopReplicaRequest或UpdateMetadataRequest中的一种
        if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA)
          throw new KafkaException(s"Unexpected apiKey received: $apiKey")

        val response = clientResponse.responseBody

        stateChangeLogger.withControllerEpoch(controllerEpoch()).trace(s"Received response " +
          s"$response for request $api with correlation id " +
          s"${requestHeader.correlationId} sent to broker $brokerNode")

        if (callback != null) {
          // 处理回调
          callback(response)
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Controller $controllerId fails to send a request to broker $brokerNode", e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    try {
      if (!NetworkClientUtils.isReady(networkClient, brokerNode, time.milliseconds())) {
        if (!NetworkClientUtils.awaitReady(networkClient, brokerNode, time, socketTimeoutMs))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info(s"Controller $controllerId connected to $brokerNode for sending state change requests")
      }

      true
    } catch {
      case e: Throwable =>
        warn(s"Controller $controllerId's connection to broker $brokerNode was unsuccessful", e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

  override def initiateShutdown(): Boolean = {
    if (super.initiateShutdown()) {
      networkClient.initiateClose()
      true
    } else
      false
  }
}

class ControllerBrokerRequestBatch(
  config: KafkaConfig,
  controllerChannelManager: ControllerChannelManager,
  controllerEventManager: ControllerEventManager,
  controllerContext: ControllerContext,
  stateChangeLogger: StateChangeLogger
) extends AbstractControllerBrokerRequestBatch(
  config,
  () => controllerContext,
  () => config.interBrokerProtocolVersion,
  stateChangeLogger
) {

  private def sendEvent(event: ControllerEvent): Unit = {
    controllerEventManager.put(event)
  }
  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }

  override def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit = {
    sendEvent(LeaderAndIsrResponseReceived(response, broker))
  }

  override def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int): Unit = {
    sendEvent(UpdateMetadataResponseReceived(response, broker))
  }

  override def handleStopReplicaResponse(stopReplicaResponse: StopReplicaResponse, brokerId: Int,
                                         partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit = {
    if (partitionErrorsForDeletingTopics.nonEmpty)
      sendEvent(TopicDeletionStopReplicaResponseReceived(brokerId, stopReplicaResponse.error, partitionErrorsForDeletingTopics))
  }
}

/**
 * Structure to send RPCs from controller to broker to inform about the metadata and leadership
 * changes in the system.
 * @param config Kafka config present in the controller.
 * @param metadataProvider Provider to provide the relevant metadata to build the state needed to
 *                         send RPCs
 * @param metadataVersionProvider Provider to provide the metadata version used by the controller.
 * @param stateChangeLogger logger to log the various events while sending requests and receiving
 *                          responses from the brokers
 * @param kraftController whether the controller is KRaft controller
 */
abstract class AbstractControllerBrokerRequestBatch(config: KafkaConfig,
                                                    metadataProvider: () => ControllerChannelContext,
                                                    metadataVersionProvider: () => MetadataVersion,
                                                    stateChangeLogger: StateChangeLogger,
                                                    kraftController: Boolean = false) extends Logging {
  val controllerId: Int = config.brokerId
  private val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, LeaderAndIsrPartitionState]]
  private val stopReplicaRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, StopReplicaPartitionState]]
  private val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]
  private val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, UpdateMetadataPartitionState]
  private var updateType: AbstractControlRequest.Type = AbstractControlRequest.Type.UNKNOWN
  private var metadataInstance: ControllerChannelContext = _

  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit

  def newBatch(): Unit = {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        s"a new one. Some LeaderAndIsr state changes $leaderAndIsrRequestMap might be lost ")
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some StopReplica state changes $stopReplicaRequestMap might be lost ")
    if (updateMetadataRequestBrokerSet.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some UpdateMetadata state changes to brokers $updateMetadataRequestBrokerSet with partition info " +
        s"$updateMetadataRequestPartitionInfoMap might be lost ")
    metadataInstance = metadataProvider()
  }

  def setUpdateType(updateType: AbstractControlRequest.Type): Unit = {
    this.updateType = updateType
  }

  def clear(): Unit = {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
    metadataInstance = null
    updateType = AbstractControlRequest.Type.UNKNOWN
  }

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int],
                                       topicPartition: TopicPartition,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicaAssignment: ReplicaAssignment,
                                       isNew: Boolean): Unit = {

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyNew = result.get(topicPartition).exists(_.isNew)
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      val partitionState = new LeaderAndIsrPartitionState()
        .setTopicName(topicPartition.topic)
        .setPartitionIndex(topicPartition.partition)
        .setControllerEpoch(leaderIsrAndControllerEpoch.controllerEpoch)
        .setLeader(leaderAndIsr.leader)
        .setLeaderEpoch(leaderAndIsr.leaderEpoch)
        .setIsr(leaderAndIsr.isr)
        .setPartitionEpoch(leaderAndIsr.partitionEpoch)
        .setReplicas(replicaAssignment.replicas.map(Integer.valueOf).asJava)
        .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
        .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
        .setIsNew(isNew || alreadyNew)

      if (metadataVersionProvider.apply().isAtLeast(IBP_3_2_IV0)) {
        partitionState.setLeaderRecoveryState(leaderAndIsr.leaderRecoveryState.value)
      }

      result.put(topicPartition, partitionState)
    }

    addUpdateMetadataRequestForBrokers(metadataInstance.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int],
                                      topicPartition: TopicPartition,
                                      deletePartition: Boolean): Unit = {
    // A sentinel (-2) is used as an epoch if the topic is queued for deletion. It overrides
    // any existing epoch.
    val leaderEpoch = metadataInstance.leaderEpoch(topicPartition)

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = stopReplicaRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyDelete = result.get(topicPartition).exists(_.deletePartition)
      result.put(topicPartition, new StopReplicaPartitionState()
          .setPartitionIndex(topicPartition.partition())
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(alreadyDelete || deletePartition))
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicPartition]): Unit = {
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    partitions.foreach { partition =>
      val beingDeleted = metadataInstance.isTopicQueuedUpForDeletion(partition.topic())
      metadataInstance.partitionLeadershipInfo(partition) match {
        case Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          val replicas = metadataInstance.partitionReplicaAssignment(partition)
          val offlineReplicas = replicas.filter(!metadataInstance.isReplicaOnline(_, partition))
          val updatedLeaderAndIsr =
            if (beingDeleted) LeaderAndIsr.duringDelete(leaderAndIsr.isr)
            else leaderAndIsr
          addUpdateMetadataRequestForBrokers(brokerIds, controllerEpoch, partition,
            updatedLeaderAndIsr.leader, updatedLeaderAndIsr.leaderEpoch, updatedLeaderAndIsr.partitionEpoch,
            updatedLeaderAndIsr.isr.asScala.map(_.toInt).toList, replicas, offlineReplicas)
        case None =>
          info(s"Leader not yet assigned for partition $partition. Skip sending UpdateMetadataRequest.")
      }
    }
  }

  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int]): Unit = {
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
  }

  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         controllerEpoch: Int,
                                         partition: TopicPartition,
                                         leader: Int,
                                         leaderEpoch: Int,
                                         partitionEpoch: Int,
                                         isrs: List[Int],
                                         replicas: Seq[Int],
                                         offlineReplicas: Seq[Int]): Unit = {
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    val partitionStateInfo = new UpdateMetadataPartitionState()
      .setTopicName(partition.topic)
      .setPartitionIndex(partition.partition)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isrs.map(Integer.valueOf).asJava)
      .setZkVersion(partitionEpoch)
      .setReplicas(replicas.map(Integer.valueOf).asJava)
      .setOfflineReplicas(offlineReplicas.map(Integer.valueOf).asJava)
    updateMetadataRequestPartitionInfoMap.put(partition, partitionStateInfo)
  }

  private def sendLeaderAndIsrRequest(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    val metadataVersion = metadataVersionProvider.apply()
    val leaderAndIsrRequestVersion: Short =
      if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 7
      else if (metadataVersion.isAtLeast(IBP_3_2_IV0)) 6
      else if (metadataVersion.isAtLeast(IBP_2_8_IV1)) 5
      else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 4
      else if (metadataVersion.isAtLeast(IBP_2_4_IV0)) 3
      else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 2
      else if (metadataVersion.isAtLeast(IBP_1_0_IV0)) 1
      else 0

    leaderAndIsrRequestMap.foreachEntry { (broker, leaderAndIsrPartitionStates) =>
      if (metadataInstance.liveOrShuttingDownBrokerIds.contains(broker)) {
        val leaderIds = mutable.Set.empty[Int]
        var numBecomeLeaders = 0
        leaderAndIsrPartitionStates.foreachEntry { (topicPartition, state) =>
          leaderIds += state.leader
          val typeOfRequest = if (broker == state.leader) {
            numBecomeLeaders += 1
            "become-leader"
          } else {
            "become-follower"
          }
          if (stateChangeLog.isTraceEnabled)
            stateChangeLog.trace(s"Sending $typeOfRequest LeaderAndIsr request $state to broker $broker for partition $topicPartition")
        }
        stateChangeLog.info(s"Sending LeaderAndIsr request to broker $broker with $numBecomeLeaders become-leader " +
          s"and ${leaderAndIsrPartitionStates.size - numBecomeLeaders} become-follower partitions")
        val leaders = metadataInstance.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.node(config.interBrokerListenerName)
        }
        val brokerEpoch = metadataInstance.liveBrokerIdAndEpochs(broker)
        val topicIds = leaderAndIsrPartitionStates.keys
          .map(_.topic)
          .toSet[String]
          .map(topic => (topic, metadataInstance.topicIds.getOrElse(topic, Uuid.ZERO_UUID)))
          .toMap
        val leaderAndIsrRequestBuilder = new LeaderAndIsrRequest.Builder(
          leaderAndIsrRequestVersion,
          controllerId,
          controllerEpoch,
          brokerEpoch,
          leaderAndIsrPartitionStates.values.toBuffer.asJava,
          topicIds.asJava,
          leaders.asJava,
          kraftController,
          updateType
        )
        sendRequest(broker, leaderAndIsrRequestBuilder, (r: AbstractResponse) => {
          val leaderAndIsrResponse = r.asInstanceOf[LeaderAndIsrResponse]
          handleLeaderAndIsrResponse(leaderAndIsrResponse, broker)
        })
      }
    }
    leaderAndIsrRequestMap.clear()
  }

  def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit

  private def sendUpdateMetadataRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    stateChangeLog.info(s"Sending UpdateMetadata request to brokers $updateMetadataRequestBrokerSet " +
      s"for ${updateMetadataRequestPartitionInfoMap.size} partitions")

    val partitionStates = updateMetadataRequestPartitionInfoMap.values.toBuffer
    val metadataVersion = metadataVersionProvider.apply()
    val updateMetadataRequestVersion: Short =
      if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 8
      else if (metadataVersion.isAtLeast(IBP_2_8_IV1)) 7
      else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 6
      else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 5
      else if (metadataVersion.isAtLeast(IBP_1_0_IV0)) 4
      else if (metadataVersion.isAtLeast(IBP_0_10_2_IV0)) 3
      else if (metadataVersion.isAtLeast(IBP_0_10_0_IV1)) 2
      else if (metadataVersion.isAtLeast(IBP_0_9_0)) 1
      else 0

    val liveBrokers = metadataInstance.liveOrShuttingDownBrokers.iterator.map { broker =>
      val endpoints = if (updateMetadataRequestVersion == 0) {
        // Version 0 of UpdateMetadataRequest only supports PLAINTEXT
        val securityProtocol = SecurityProtocol.PLAINTEXT
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val node = broker.node(listenerName)
        Seq(new UpdateMetadataEndpoint()
          .setHost(node.host)
          .setPort(node.port)
          .setSecurityProtocol(securityProtocol.id)
          .setListener(listenerName.value))
      } else {
        broker.endPoints.map { endpoint =>
          new UpdateMetadataEndpoint()
            .setHost(endpoint.host)
            .setPort(endpoint.port)
            .setSecurityProtocol(endpoint.securityProtocol.id)
            .setListener(endpoint.listenerName.value)
        }
      }
      new UpdateMetadataBroker()
        .setId(broker.id)
        .setEndpoints(endpoints.asJava)
        .setRack(broker.rack.orNull)
    }.toBuffer

    updateMetadataRequestBrokerSet.intersect(metadataInstance.liveOrShuttingDownBrokerIds).foreach { broker =>
      val brokerEpoch = metadataInstance.liveBrokerIdAndEpochs(broker)
      val topicIds = partitionStates.map(_.topicName())
        .distinct
        .filter(metadataInstance.topicIds.contains)
        .map(topic => (topic, metadataInstance.topicIds(topic))).toMap
      val updateMetadataRequestBuilder = new UpdateMetadataRequest.Builder(
        updateMetadataRequestVersion,
        controllerId,
        controllerEpoch,
        brokerEpoch,
        partitionStates.asJava,
        liveBrokers.asJava,
        topicIds.asJava,
        kraftController,
        updateType
      )
      sendRequest(broker, updateMetadataRequestBuilder, (r: AbstractResponse) => {
        val updateMetadataResponse = r.asInstanceOf[UpdateMetadataResponse]
        handleUpdateMetadataResponse(updateMetadataResponse, broker)
      })

    }
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int): Unit

  private def sendStopReplicaRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    val traceEnabled = stateChangeLog.isTraceEnabled
    val metadataVersion = metadataVersionProvider.apply()
    val stopReplicaRequestVersion: Short =
      if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 4
      else if (metadataVersion.isAtLeast(IBP_2_6_IV0)) 3
      else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 2
      else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 1
      else 0

    def responseCallback(brokerId: Int, isPartitionDeleted: TopicPartition => Boolean)
                        (response: AbstractResponse): Unit = {
      val stopReplicaResponse = response.asInstanceOf[StopReplicaResponse]
      val partitionErrorsForDeletingTopics = mutable.Map.empty[TopicPartition, Errors]
      stopReplicaResponse.partitionErrors.forEach { pe =>
        val tp = new TopicPartition(pe.topicName, pe.partitionIndex)
        if (metadataInstance.isTopicDeletionInProgress(pe.topicName) &&
            isPartitionDeleted(tp)) {
          partitionErrorsForDeletingTopics += tp -> Errors.forCode(pe.errorCode)
        }
      }
      if (partitionErrorsForDeletingTopics.nonEmpty)
        handleStopReplicaResponse(stopReplicaResponse, brokerId, partitionErrorsForDeletingTopics.toMap)
    }

    stopReplicaRequestMap.foreachEntry { (brokerId, partitionStates) =>
      if (metadataInstance.liveOrShuttingDownBrokerIds.contains(brokerId)) {
        if (traceEnabled)
          partitionStates.foreachEntry { (topicPartition, partitionState) =>
            stateChangeLog.trace(s"Sending StopReplica request $partitionState to " +
              s"broker $brokerId for partition $topicPartition")
          }

        val brokerEpoch = metadataInstance.liveBrokerIdAndEpochs(brokerId)
        if (stopReplicaRequestVersion >= 3) {
          val stopReplicaTopicState = mutable.Map.empty[String, StopReplicaTopicState]
          partitionStates.foreachEntry { (topicPartition, partitionState) =>
            val topicState = stopReplicaTopicState.getOrElseUpdate(topicPartition.topic,
              new StopReplicaTopicState().setTopicName(topicPartition.topic))
            topicState.partitionStates().add(partitionState)
          }

          stateChangeLog.info(s"Sending StopReplica request for ${partitionStates.size} " +
            s"replicas to broker $brokerId")
          val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
            stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
            false, stopReplicaTopicState.values.toBuffer.asJava, kraftController)
          sendRequest(brokerId, stopReplicaRequestBuilder,
            responseCallback(brokerId, tp => partitionStates.get(tp).exists(_.deletePartition)))
        } else {
          var numPartitionStateWithDelete = 0
          var numPartitionStateWithoutDelete = 0
          val topicStatesWithDelete = mutable.Map.empty[String, StopReplicaTopicState]
          val topicStatesWithoutDelete = mutable.Map.empty[String, StopReplicaTopicState]

          partitionStates.foreachEntry { (topicPartition, partitionState) =>
            val topicStates = if (partitionState.deletePartition()) {
              numPartitionStateWithDelete += 1
              topicStatesWithDelete
            } else {
              numPartitionStateWithoutDelete += 1
              topicStatesWithoutDelete
            }
            val topicState = topicStates.getOrElseUpdate(topicPartition.topic,
              new StopReplicaTopicState().setTopicName(topicPartition.topic))
            topicState.partitionStates().add(partitionState)
          }

          if (topicStatesWithDelete.nonEmpty) {
            stateChangeLog.info(s"Sending StopReplica request (delete = true) for " +
              s"$numPartitionStateWithDelete replicas to broker $brokerId")
            val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
              stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
              true, topicStatesWithDelete.values.toBuffer.asJava, kraftController)
            sendRequest(brokerId, stopReplicaRequestBuilder, responseCallback(brokerId, _ => true))
          }

          if (topicStatesWithoutDelete.nonEmpty) {
            stateChangeLog.info(s"Sending StopReplica request (delete = false) for " +
              s"$numPartitionStateWithoutDelete replicas to broker $brokerId")
            val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
              stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
              false, topicStatesWithoutDelete.values.toBuffer.asJava, kraftController)
            sendRequest(brokerId, stopReplicaRequestBuilder)
          }
        }
      }
    }

    stopReplicaRequestMap.clear()
  }

  def handleStopReplicaResponse(stopReplicaResponse: StopReplicaResponse, brokerId: Int,
                                partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit

  def sendRequestsToBrokers(controllerEpoch: Int): Unit = {
    try {
      val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)
      sendLeaderAndIsrRequest(controllerEpoch, stateChangeLog)
      sendUpdateMetadataRequests(controllerEpoch, stateChangeLog)
      sendStopReplicaRequests(controllerEpoch, stateChangeLog)
      this.updateType = AbstractControlRequest.Type.UNKNOWN
    } catch {
      case e: Throwable =>
        if (leaderAndIsrRequestMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " +
            s"the map is $leaderAndIsrRequestMap. Exception message: $e")
        }
        if (updateMetadataRequestBrokerSet.nonEmpty) {
          error(s"Haven't been able to send metadata update requests to brokers $updateMetadataRequestBrokerSet, " +
            s"current state of the partition info is $updateMetadataRequestPartitionInfoMap. Exception message: $e")
        }
        if (stopReplicaRequestMap.nonEmpty) {
          error("Haven't been able to send stop replica requests, current state of " +
            s"the map is $stopReplicaRequestMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
    }
  }
}

case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                    // 目标 Broker 节点对象，里面封装了目标 Broker 的连接信息，比如主机名、端口号等。
                                     brokerNode: Node,
                                    //请求消息阻塞队列。你可以发现，Controller 为每个目标 Broker 都创建了一个消息队列
                                     messageQueue: BlockingQueue[QueueItem],
                                    // Controller 使用这个线程给目标 Broker 发送请求
                                     requestSendThread: RequestSendThread,
                                     queueSizeGauge: Gauge[Int],
                                     requestRateAndTimeMetrics: Timer,
                                     reconfigurableChannelBuilder: Option[Reconfigurable])

