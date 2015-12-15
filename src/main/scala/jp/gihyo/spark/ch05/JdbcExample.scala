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
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object JdbcExample {

  /**
   * An example to create a `DataFrame` from a MySQL table through the JDBC driver
   *
   * You should prepare a MySQL table for this example.
   * You can create it by ./ch05/ch05-mysql-example.sql
   *
   * Run with
   * {{{
   * spark-submit --class jp.gihyo.spark.ch05.JdbcExample \
   *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
   *     "jdbc:mysql://localhost/gihyo_spark" "MYSQL_USER" "MYSQL_PASS"
   * }}}
   */
  def main(args: Seq[String]): Unit = {
    if (args.length != 3) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }
    val url = args(0)
    val user = args(1)
    val pass = args(2)

    val conf = new SparkConf().setAppName("JdbcExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    run(sc, sqlContext, url, user, pass)

    sc.stop()
  }

  def run(sc: SparkContext, sqlContext: SQLContext,
      url: String, user: String, pass: String): Unit = {
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", pass)

    val df: DataFrame = sqlContext.read.jdbc(url, "gihyo_spark.person", prop)
    df.printSchema()
    println("# Rows: " + df.count())
  }
}
// scalastyle:on println
