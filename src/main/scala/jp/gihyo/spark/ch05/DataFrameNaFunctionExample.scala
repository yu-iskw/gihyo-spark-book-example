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

package jp.gihyo.spark.ch05

// scalastyle:off println
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * An example code for `DataFrameNaFunction`
 *
 * Run with
 * {{{
 * spark-submit --class jp.gihyo.spark.ch05.DataFrameNaFunctionExample \
 *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar
 * }}}
 */
object DataFrameNaFunctionExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BasicDataFrameExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    run(sc, sqlContext)
    sc.stop()
  }

  def run(
      sc: SparkContext,
      sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._

    val nullDF = Seq[(String, java.lang.Integer, java.lang.Double)](
      ("Bob", 16, 176.5),
      ("Alice", null, 164.3),
      ("", 60, null),
      ("UNKNOWN", 25, Double.NaN),
      ("Amy", null, null),
      (null, null, Double.NaN)
    ).toDF("name", "age", "height")

    // drop
    nullDF.na.drop("any").show()
    nullDF.na.drop("all").show()
    nullDF.na.drop(Array("age")).show()
    nullDF.na.drop(Seq("age", "height")).show()
    nullDF.na.drop("any", Array("name", "age")).show()
    nullDF.na.drop("all", Array("age", "height")).show()

    // fill
    nullDF.na.fill(0.0, Array("name", "height")).show()
    nullDF.na.fill(Map(
      "name" -> "UNKNOWN",
      "height" -> 0.0
    )).show()

    // replace
    nullDF.na.replace("name", Map("" -> "UNKNOWN")).show()
  }
}

// scalastyle:on println
