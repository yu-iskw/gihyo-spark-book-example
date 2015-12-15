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

package jp.gihyo.spark.ch03.shared_variable

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

object WordCountExample {
  def main(args: Array[String]) {
    if (args.length != 1) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("WordCountExample")
    val sc = new SparkContext(conf)

    run(sc, args(0))
    sc.stop()
  }

  def run(sc: SparkContext, inputFile: String) {
    val stopWordCount = sc.accumulator(0L)
    val stopWords = sc.broadcast(Set("a", "an", "for", "in", "on"))

    val lines = sc.textFile(inputFile)
    val words = lines.flatMap(_.split(" ")).filter(!_.isEmpty)
    val wordCounts = words.map(w => (w, 1)).reduceByKey(_ + _).filter { w =>
      val result = !stopWords.value.contains(w._1)
      if (!result) stopWordCount += 1L
      result
    }
    val sortedWordCounts = wordCounts.sortBy(_._2, ascending = false)

    println(s"""wordCounts:     ${sortedWordCounts.take(10).mkString(", ")}""")
    println(s"""stopWordCounts: ${stopWordCount.value}""")
  }
}

// scalastyle:on println
