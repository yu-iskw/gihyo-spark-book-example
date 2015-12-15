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

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object gihyo_6_3_Union {
  def main(args: Array[String]) {
    if (args.length != 3) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }
    val targetHosts = args(0)
    val consumerGroup = args(1)
    val targetTopics = args(2)

    val conf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val KafkaStreams = (1 to 5).map { i =>
      KafkaUtils.createStream(ssc, targetHosts, consumerGroup, Map(targetTopics -> 1))
    }
    run(ssc, KafkaStreams)

    ssc.start
    ssc.awaitTermination
  }

  def run(ssc: StreamingContext, streams: IndexedSeq[InputDStream[(String, String)]]) {
    val unionedStream = ssc.union(streams)
    unionedStream.print
  }
}
