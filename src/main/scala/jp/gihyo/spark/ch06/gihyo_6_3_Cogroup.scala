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
import org.apache.spark.streaming.dstream.InputDStream


object gihyo_6_3_Cogroup {
  def main(args: Array[String]) {
    if (args.length != 4) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }
    val targetHost1 = args(0)
    val targetHostPort1 = args(1).toInt
    val targetHost2 = args(2)
    val targetHostPort2 = args(3).toInt

    val conf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines1 = ssc.socketTextStream(targetHost1, targetHostPort1)
    val lines2 = ssc.socketTextStream(targetHost2, targetHostPort2)
    run(lines1, lines2)

    ssc.start
    ssc.awaitTermination
  }

  def run(stream: InputDStream[String], otherStream: InputDStream[String]) {
    val lines1KV = stream.map(x => (x, "attribute1"))
    val lines2KV = otherStream.map(x => (x, "attribute2"))
    val linesKVW = lines1KV.cogroup(lines2KV)
    linesKVW.print
  }
}
