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
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

object gihyo_6_3_KafkaStream {
  def main(args: Array[String]) {
    if (args.length != 4) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }
    val brokerList = args(0)
    val consumeTopic = args(1)
    val checkpointDir = args(2)
    val saveDir = args(3)

    val f = createStreamingContext(brokerList, consumeTopic, checkpointDir, saveDir)
    // StreamingContextの取得
    val ssc = StreamingContext.getOrCreate(checkpointDir, f)

    sys.ShutdownHookThread {
      System.out.println("Gracefully stopping SparkStreaming Application")
      ssc.stop(true, true)
      System.out.println("SparkStreaming Application stopped")
    }
    ssc.start
    ssc.awaitTermination
  }

  def createStreamingContext(brokerList: String,
      consumeTopic: String,
      checkpointDir: String,
      saveDir: String): () => StreamingContext = { () => {
      /*
       * StreamingContextの生成メソッド
       */
      val conf = new SparkConf().setAppName("gihyoSample_Application")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(5))
      ssc.checkpoint(checkpointDir)

      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
      val kafkaStream = KafkaUtils
        .createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(consumeTopic))
      run(kafkaStream, saveDir)
      ssc
    }
  }

  def updateStateByKeyFunction(values: Seq[Long], running: Option[Int]): Option[Int] = {
    /*
     * userID毎にアクセス数をcountする関数
     */
    System.out.println(values)
    Some(running.getOrElse(0) + values.length)
  }

  def run(stream: InputDStream[(String, String)],
    saveDir: String, windowLength: Int = 30, slideInterval: Int = 5) {
    val baseStream = stream.transform(rdd => {
      val t = (Long.MaxValue - System.currentTimeMillis)
      rdd.map(x => (x._1, x._2 + ", " + t))
    }).map(x => {
      val splitVal = x._2.split(",")
      val userVal = splitVal(0).split(":")
      val actionVal = splitVal(1).split(":")
      val pageVal = splitVal(2).split(":")
      val timestamp = splitVal(3)
      (actionVal(1), userVal(1), pageVal(1), timestamp)
    })
    baseStream.persist()

    val accountStream = baseStream.filter(_._1 == "view")
      .map(x => x._2)
      .countByValue()

    val totalUniqueUser = accountStream
      .updateStateByKey[Int](updateStateByKeyFunction _)
      .count()
      .map(x => "totalUniqueUser:" + x)

    val baseStreamPerTirty = baseStream
      .window(Seconds(windowLength), Seconds(slideInterval))
      .filter(_._1 == "view")
    baseStreamPerTirty.persist()

    val pageViewPerTirty = baseStreamPerTirty
      .count()
      .map(x => "PageView:" + x)

    val uniqueUserPerTirty = baseStreamPerTirty
      .map(x => x._2)
      .countByValue()
      .count()
      .map(x => "UniqueUser:" + x)

    val pageViewStream = baseStream
      .filter(_._1 == "view")
      .map(x => x._3)
      .count()
      .map(x => "PageView:" + x)

    val outputStream = totalUniqueUser
      .union(pageViewPerTirty)
      .union(uniqueUserPerTirty)
      .union(pageViewStream)
      .reduce((x, y) => x + ", " + y)
      .saveAsTextFiles(saveDir)
  }
}

// scalastyle:on println
