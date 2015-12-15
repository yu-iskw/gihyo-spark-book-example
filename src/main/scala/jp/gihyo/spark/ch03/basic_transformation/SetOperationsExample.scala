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

object SetOperationsExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("SetOperationsExample")
    val sc = new SparkContext(conf)

    run(sc)
    sc.stop()
  }

  def run(sc: SparkContext) {
    val fruits1 = sc.parallelize(Array("Apple", "Orange", "Peach", "Orange", "PineApple", "Orange"))
    val fruits2 = sc.parallelize(Array("Grape", "Apple", "Banana", "Orange"))

    val union = fruits1.union(fruits2)
    val subtract = fruits1.subtract(fruits2)
    val intersection = fruits1.intersection(fruits2)
    val cartesian = fruits1.cartesian(fruits2)

    println(s"""fruits1: ${fruits1.collect().mkString(", ")}""")
    println(s"""fruits2: ${fruits2.collect().mkString(", ")}""")
    println(s"""union: ${union.collect().mkString(", ")}""")
    println(s"""subtract: ${subtract.collect().mkString(", ")}""")
    println(s"""intersection: ${intersection.collect().mkString(", ")}""")
    println(s"""cartesian: ${cartesian.collect().mkString(", ")}""")
  }
}

// scalastyle:on println
