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

package jp.gihyo.spark.ch03.basic_transformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

object ZipExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("ZipExample")
    val sc = new SparkContext(conf)

    run(sc)
    sc.stop()
  }

  def run(sc: SparkContext) {
    val fruits1 = sc.parallelize(
      Array("Apple", "Orange", "Peach", "Orange", "PineApple", "Orange"))
    val fruits2 = sc.parallelize(
      Array("りんご", "オレンジ", "桃", "オレンジ", "パイナップル", "オレンジ"))
    val zipped = fruits1.zip(fruits2)

    println(s"""fruits1: ${fruits1.collect().mkString(", ")}""")
    println(s"""fruits2: ${fruits2.collect().mkString(", ")}""")
    println(s"""zipped:  ${zipped.collect().mkString(", ")}""")
  }
}

// scalastyle:on println
