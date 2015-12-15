/*
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
package jp.gihyo.spark

import org.scalatest.{BeforeAndAfterEach, Suite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import jp.gihyo.spark.ch06.UserDic

private[spark]
trait TestStreamingContext extends BeforeAndAfterEach { self: Suite =>
  @transient var ssc: StreamingContext = _
  @transient var sc: SparkContext = _
  val master = "local[2]"
  val appN = "StreamingUnitTest"
  val bd = Seconds(1)

  override def beforeEach() {
    super.beforeEach()
    val conf = new SparkConf().setMaster(master)
      .setAppName(appN)
      .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .registerKryoClasses(Array(classOf[UserDic]))

    ssc = new StreamingContext(conf, bd)
    sc = ssc.sparkContext
  }

  override def afterEach() {
    try {
      if (ssc != null) {
        // stop with sc
        ssc.stop(true)
      }
      ssc = null;
    } finally {
      super.afterEach()
    }
  }
}
