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
package jp.gihyo.spark.ch06

import jp.gihyo.spark.{SparkFunSuite, TestStreamingContext}
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContextWrapper

class gihyo_6_3_JoinSuite extends SparkFunSuite with TestStreamingContext {

  test("run") {
    val lines1 = mutable.Queue[RDD[String]]()
    val ds1 = ssc.queueStream(lines1)
    val lines2 = mutable.Queue[RDD[String]]()
    val ds2 = ssc.queueStream(lines2)
    val clock = new StreamingContextWrapper(ssc).manualClock
    gihyo_6_3_Join.run(ds1, ds2)
    ssc.start()

    lines1 += sc.makeRDD(Seq("key1", "key2", "key3")) // test data
    lines2 += sc.makeRDD(Seq("key2", "key3", "key4")) // test data
    clock.advance(1000)
    Thread.sleep(1000)
  }
}
