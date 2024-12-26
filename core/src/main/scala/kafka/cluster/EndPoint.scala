/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.cluster

import org.apache.kafka.common.{KafkaException, Endpoint => JEndpoint}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils

import java.util.Locale

object EndPoint {
  def parseListenerName(connectionString: String): String = {
    val firstColon = connectionString.indexOf(':')
    if (firstColon < 0) {
      throw new KafkaException(s"Unable to parse a listener name from $connectionString")
    }
    connectionString.substring(0, firstColon).toUpperCase(Locale.ROOT)
  }

  def fromJava(endpoint: JEndpoint): EndPoint =
    new EndPoint(endpoint.host(),
      endpoint.port(),
      new ListenerName(endpoint.listenerName().get()),
      endpoint.securityProtocol())
}

/**
 * Part of the broker definition - matching host/port pair to a protocol
 */
case class EndPoint(host: String, port: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol) {
  // 构造完整的监听器连接字符串
  // 格式为：监听器名称://主机名：端口
  // 比如：PLAINTEXT://kafka-host:9092
  def connectionString: String = {
    val hostport =
      if (host == null)
        ":"+port
      else
        Utils.formatAddress(host, port)
    listenerName.value + "://" + hostport
  }

  // clients工程下有一个Java版本的Endpoint类供clients端代码使用
  // 此方法是构造Java版本的Endpoint类实例
  def toJava: JEndpoint = {
    new JEndpoint(listenerName.value, securityProtocol, host, port)
  }
}
