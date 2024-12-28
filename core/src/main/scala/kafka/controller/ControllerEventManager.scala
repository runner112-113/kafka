/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import com.yammer.metrics.core.Timer

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.ShutdownableThread

import scala.collection._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  private val EventQueueTimeMetricName = "EventQueueTimeMs"
  private val EventQueueSizeMetricName = "EventQueueSize"
}

// Controller 端的事件处理器接口
trait ControllerEventProcessor {
  /**
   * 接收一个 Controller 事件，并进行处理
   * @param event
   */
  def process(event: ControllerEvent): Unit

  /**
   * 接收一个 Controller 事件，并抢占队列之前的事件进行优先处理
   * @param event
   */
  def preempt(event: ControllerEvent): Unit
}

class QueuedEvent(val event: ControllerEvent, // 表示Controller事件
                  val enqueueTimeMs: Long) {// 表示Controller事件被放入到事件队列的时间戳
  // 标识事件是否开始被处理
  // 在这里，QueuedEvent 使用它的唯一目的，是确保 Expire 事件在建立 ZooKeeper 会话前被处理
  private val processingStarted = new CountDownLatch(1)
  // 标识事件是否被处理过
  private val spent = new AtomicBoolean(false)

  // 处理事件
  def process(processor: ControllerEventProcessor): Unit = {
    // 若已经被处理过，直接返回
    if (spent.getAndSet(true))
      return
    processingStarted.countDown()
    // 调用ControllerEventProcessor的process方法处理事件
    processor.process(event)
  }

  // 抢占式处理事件
  def preempt(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processor.preempt(event)
  }

  // 阻塞等待事件被处理完成
  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

// 定义各种Controller事件以及这些事件的处理
// 事件处理器，用于创建和管理 ControllerEventThread
class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, Timer],
                             eventQueueTimeTimeoutMs: Long = 300000) {
  import ControllerEventManager._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // Visible for test
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

  private val eventQueueTimeHist = metricsGroup.newHistogram(EventQueueTimeMetricName)

  metricsGroup.newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      metricsGroup.removeMetric(EventQueueTimeMetricName)
      metricsGroup.removeMetric(EventQueueSizeMetricName)
    }
  }

  /**
   * 把指定 ControllerEvent 插入到事件队列
   * @param event
   * @return
   */
  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    // 构建QueuedEvent实例
    val queuedEvent = new QueuedEvent(event, time.milliseconds())

    queue.put(queuedEvent)
    queuedEvent
  }

  /**
   * 先执行具有高优先级的抢占式事件，之后清空队列所有事件，最后再插入指定的事件。
   * @param event
   * @return
   */
  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val preemptedEvents = new util.ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents)
    // 优先处理抢占式事件
    preemptedEvents.forEach(_.preempt(processor))
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  // 专属的事件处理线程，唯一的作用是处理不同种类的 ControllEvent
  class ControllerEventThread(name: String)
    extends ShutdownableThread(
      name, false, s"[ControllerEventThread controllerId=$controllerId] ")
      with Logging {

    logIdent = logPrefix

    override def doWork(): Unit = {
      // 从事件队列中获取待处理的Controller事件，否则等待
      val dequeued = pollFromEventQueue()
      dequeued.event match {
        // 如果是关闭线程事件，什么都不用做。关闭线程由外部来执行
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state

          // 更新对应事件在队列中保存的时间
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            def process(): Unit = dequeued.process(processor)

            // 处理事件，同时计算处理速率
            rateAndTimeMetrics.get(state) match {
              case Some(timer) => timer.time(() => process())
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

  private def pollFromEventQueue(): QueuedEvent = {
    val count = eventQueueTimeHist.count()
    if (count != 0) {
      val event  = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        queue.take()
      } else {
        event
      }
    } else {
      queue.take()
    }
  }

}
