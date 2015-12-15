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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

object MapPartitionsExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("MapPartitionsExample")
    val sc = new SparkContext(conf)

    run(sc)
    sc.stop()
  }

  def run(sc: SparkContext) {
    val jsonLines = sc.parallelize(Array(
      """{"name": "Apple",  "num": 1}""",
      """{"name": "Orange", "num": 4}""",
      """{"name": "Apple",  "num": 2}""",
      """{"name": "Peach",  "num": 1}"""
    ))

    val parsed = jsonLines.mapPartitions { lines =>
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      lines.map { line =>
        val f = mapper.readValue(line, classOf[Map[String, String]])
        (f("name"), f("num"))
      }
    }

    println(s"""json:\n${jsonLines.collect().mkString("\n")}""")
    println()
    println(s"""parsed:\n${parsed.collect().mkString("\n")}""")
  }
}

// scalastyle:on println
