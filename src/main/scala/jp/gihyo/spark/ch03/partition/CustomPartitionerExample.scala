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

package jp.gihyo.spark.ch03.partition

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

object CustomPartitionerExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("CustomPartitionerExample")
    val sc = new SparkContext(conf)

    run(sc)
    sc.stop()
  }

  def run(sc: SparkContext) {
    val fruits = sc.parallelize(Array("Apple", "Orange", "Peach", "Orange", "PineApple", "Orange"))

    val defaultPartitioned = fruits.map((_, 1)).reduceByKey(_ + _)
    val customPartitioned = fruits.map((_, 1)).reduceByKey(
      new FirstLetterPartitioner(sc.defaultParallelism), _ + _)

    println(s"""fruits:\n  ${fruits.collect().mkString(", ")}""")
    println()

    println("partitioned by default partitioner")
    defaultPartitioned.glom().mapPartitionsWithIndex((p, it) =>
      it.map(n => s"""  Par$p: ${n.mkString(",")}""")
    ).foreach(println)
    println()

    println("partitioned by first letter partitioner")
    customPartitioned.glom().mapPartitionsWithIndex((p, it) =>
      it.map(n => s"""  Par$p: ${n.mkString(",")}""")
    ).foreach(println)
  }
}

private[partition]
class FirstLetterPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    key.toString.charAt(0).hashCode % numPartitions match {
      case p if p < 0 => p + numPartitions
      case p => p
    }
  }

  override def equals(other: Any): Boolean = {
    other match {
      case p: FirstLetterPartitioner => p.numPartitions == numPartitions
      case _ => false
    }
  }
}

// scalastyle:on println
