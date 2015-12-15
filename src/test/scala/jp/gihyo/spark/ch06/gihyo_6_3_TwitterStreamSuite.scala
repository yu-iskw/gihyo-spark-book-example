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

import java.nio.file.Files

import scala.collection.mutable
import scala.io.Source

import twitter4j.{Status, TwitterObjectFactory}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContextWrapper

import jp.gihyo.spark.{SparkFunSuite, TestStreamingContext}


class gihyo_6_3_TwitterStreamSuite extends SparkFunSuite with TestStreamingContext {

  test("run") {
    val lines = mutable.Queue[RDD[Status]]()
    val ds = ssc.queueStream(lines)
    val clock = new StreamingContextWrapper(ssc).manualClock
    gihyo_6_3_TwitterStream.run(
      sc,
      ds,
      Files.createTempDirectory("TwitterTag").toString,
      Files.createTempDirectory("TwitterWords").toString)
    val checkpointDir = Files.createTempDirectory("StreamingUnitTest").toString
    ssc.checkpoint(checkpointDir)
    ssc.start()

    (1 to 2).foreach { case i =>
      // test data
      lines += sc.makeRDD(Seq(
        MockTweetGenerator.createMockStatusFromJson(),
        MockTweetGenerator.createMockStatusFromJson(),
        MockTweetGenerator.createMockStatusFromJson(),
        MockTweetGenerator.createMockStatusFromJson()))
      clock.advance(1000)
      Thread.sleep(1000)
    }
  }
}

object MockTweetGenerator {
  // Creates a tweet status from a JSON file
  def createMockStatusFromJson(): Status = {
    val jsonFile = getClass.getResource("/streaming/test-tweet.json").getPath
    TwitterObjectFactory.createStatus(Source.fromFile(jsonFile).getLines().mkString)
  }
}
