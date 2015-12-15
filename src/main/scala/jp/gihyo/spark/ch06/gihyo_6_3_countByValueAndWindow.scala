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

// scalastyle:off println
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.dstream.InputDStream

object gihyo_6_3_countByValueAndWindow {
  def main(args: Array[String]) {
    if (args.length != 3) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }
    val targetHost = args(0)
    val targetHostPort = args(1).toInt
    val checkpointDir = args(2)

    val f = createStreamingContext(targetHost, targetHostPort, checkpointDir)
    val ssc = StreamingContext.getOrCreate(checkpointDir, f)

    sys.ShutdownHookThread {
      System.out.println("Gracefully stopping SparkStreaming Application")
      ssc.stop(true, true)
      System.out.println("SparkStreaming Application stopped")
    }
    ssc.start
    ssc.awaitTermination
  }

  def createStreamingContext(
      targetHost: String,
      targetHostPort: Int, checkpointDir: String): () => StreamingContext = { () => {
    /*
     * StreamingContextの生成メソッド
     */
    val conf = new SparkConf().setAppName("gihyoSample_Application")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint(checkpointDir)

    val lines = ssc.socketTextStream(targetHost, targetHostPort)
    run(lines)
    ssc
  }
  }

  def run(stream: InputDStream[String], windowLength: Int = 10, slideInterval: Int = 5) {
    val userList = stream.countByValueAndWindow(Seconds(windowLength), Seconds(slideInterval))
    userList.print
  }
}

// scalastyle:on println
