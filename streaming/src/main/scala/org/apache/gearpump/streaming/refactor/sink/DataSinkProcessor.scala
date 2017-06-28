/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

<<<<<<< HEAD:streaming/src/main/scala/org/apache/gearpump/streaming/refactor/sink/DataSinkProcessor.scala
package org.apache.gearpump.streaming.refactor.sink

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.streaming.sink.DataSink

object DataSinkProcessor {
  def apply(
      dataSink: DataSink,
      parallelism: Int = 1,
      description: String = "",
      taskConf: UserConfig = UserConfig.empty)(implicit system: ActorSystem)
    : Processor[DataSinkTask] = {
    Processor[DataSinkTask](parallelism, description = description,
      taskConf.withValue[DataSink](DataSinkTask.DATA_SINK, dataSink))
  }
=======
package org.apache.gearpump.streaming.refactor.state

import org.apache.gearpump.streaming.refactor.state.api.State

trait StateTag[StateT <: State] extends Serializable {

  def appendTo(sb: Appendable)

  def getId: String

  def getSpec: StateSpec[StateT]

  def bind(binder: StateBinder): StateT

>>>>>>> e6ce91c... [Gearpump 311] refactor state management:streaming/src/main/scala/org/apache/gearpump/streaming/refactor/state/StateTag.scala
}
