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

package jp.gihyo.spark.ch03.pairrdd_transformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

object JoinExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("JoinExample")
    val sc = new SparkContext(conf)

    run(sc)
    sc.stop()
  }

  def run(sc: SparkContext) {
    val persons = sc.parallelize(Array(
      ("Adam", "San francisco"),
      ("Bob", "San francisco"),
      ("Taro", "Tokyo"),
      ("Charles", "New York")
    ))
    val cities = sc.parallelize(Array(
      ("Tokyo", "Japan"),
      ("San francisco", "America"),
      ("Beijing", "China")
    ))

    val leftJoined = persons.map(_.swap).join(cities)
    val leftOuterJoined = persons.map(_.swap).leftOuterJoin(cities)
    val rightOuterJoined = persons.map(_.swap).rightOuterJoin(cities)
    val fullOuterJoined = persons.map(_.swap).fullOuterJoin(cities)

    println(s"""persons: ${persons.collect().mkString(", ")}""")
    println(s"""cities:  ${cities.collect().mkString(", ")}""")
    println()
    println(s"""leftJoined:\n${leftJoined.collect().mkString("\n")}""")
    println()
    println(s"""leftOuterJoined:\n${leftOuterJoined.collect().mkString("\n")}""")
    println()
    println(s"""rightOuterJoined:\n${rightOuterJoined.collect().mkString("\n")}""")
    println()
    println(s"""fullOuterJoined:\n${fullOuterJoined.collect().mkString("\n")}""")
  }
}

// scalastyle:on println
