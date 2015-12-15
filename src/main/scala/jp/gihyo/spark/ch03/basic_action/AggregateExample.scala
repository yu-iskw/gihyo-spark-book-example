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

package jp.gihyo.spark.ch03.basic_action

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

object AggregateExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("AggregateExample")
    val sc = new SparkContext(conf)

    run(sc)
    sc.stop()
  }

  private[basic_action]
  def run(sc: SparkContext) {
    val nums = sc.parallelize(Array.range(1, 11), 3)

    val acc = nums.aggregate(zeroValue = (0.0, 0))(
      seqOp = (partAcc, n) => (partAcc._1 + n, partAcc._2 + 1),
      combOp = (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    val avg = acc._1 / acc._2

    println(s"""nums: ${nums.collect().mkString(", ")}""")
    println(s"""sum:  ${nums.fold(0)((x, y) => x + y)}""")
  }
}

// scalastyle:on println
