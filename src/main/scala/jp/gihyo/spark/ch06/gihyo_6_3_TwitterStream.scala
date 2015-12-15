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

import org.atilika.kuromoji.Token
import twitter4j.Status

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object gihyo_6_3_TwitterStream {
  def main(args: Array[String]) {
    if (args.length != 7) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }

    val Array(cKey, cSecret, aToken, aSecret, cDir, tagDir, wordDir) = args

    System.setProperty("twitter4j.oauth.consumerKey", cKey)
    System.setProperty("twitter4j.oauth.consumerSecret", cSecret)
    System.setProperty("twitter4j.oauth.accessToken", aToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", aSecret)
    val f = createStreamingContext(cDir, tagDir, wordDir)
    val ssc = StreamingContext.getOrCreate(cDir, f)

    sys.ShutdownHookThread {
      System.out.println("Gracefully stopping SparkStreaming Application")
      ssc.stop(true, true)
      System.out.println("SparkStreaming Application stopped")
    }
    ssc.start
    ssc.awaitTermination
  }

  def createStreamingContext(checkpointDir: String,
      tagDir: String,
      wordDir: String): () => StreamingContext = { () => {
    /*
     * StreamingContextの生成メソッド
     */
    val conf = new SparkConf().setAppName("gihyoSample_Application")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[UserDic]))
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint(checkpointDir)
    val twitterStream = TwitterUtils.createStream(ssc, None)
    run(sc, twitterStream, tagDir, wordDir)
    ssc
  }
  }

  def run(sc: SparkContext, stream: InputDStream[Status], tagDir: String, wordDir: String) {
    val tokenizer = sc.broadcast(UserDic.getInstance)
    val tweets = stream.map(tweet => tweet.getText())
    tweets.persist()
    val TweetText = tweets
      .flatMap(text => {
        val tokens = tokenizer.value.tokenize(text).toArray
        tokens.filter(t => {
          val token = t.asInstanceOf[Token]
          ((token.getPartOfSpeech.indexOf("名詞") > -1 &&
            token.getPartOfSpeech.indexOf("一般") > -1) ||
            token.getPartOfSpeech.indexOf("カスタム名詞") > -1) &&
            token.getSurfaceForm.length > 1 &&
            !(token.getSurfaceForm matches "^[a-zA-Z]+$|^[0-9]+$")
        }).map(t => t.asInstanceOf[Token].getSurfaceForm)
      })
      .countByValue()
      .map(x => (x._2, x._1))
      .transform(_.sortByKey(false))
      .map(x => (x._2, x._1))

    val TweetTags = tweets
      .flatMap(tweet => tweet.split(" ").filter(_.startsWith("#")))
      .countByValue()
      .map(x => (x._2, x._1))
      .transform(_.sortByKey(false))
      .map(x => (x._2, x._1))

    TweetText.saveAsTextFiles(wordDir)
    TweetTags.saveAsTextFiles(tagDir)
  }
}

// scalastyle:on println
