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

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.sql.functions._

private case class Person(id: Int, name: String, age: Int)

object DatasetExample {

  /**
   * An example to create a `DataFrame` from a MySQL table through the JDBC driver
   *
   * Run with
   * {{{
   * spark-submit --class jp.gihyo.spark.ch05.DatasetExample \
   *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar
   * }}}
   */
  def main(args: Seq[String]): Unit = {
    val conf = new SparkConf().setAppName("DatasetExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    run(sc, sqlContext)
    sc.stop()
  }

  def run(sc: SparkContext, sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._

    // Creates a Dataset from a `Seq`
    val seq = Seq((1, "Bob", 23), (2, "Tom", 23), (3, "John", 22))
    val ds1: Dataset[(Int, String, Int)] = sqlContext.createDataset(seq)
    val ds2: Dataset[(Int, String, Int)] = seq.toDS()

    // Creates a Dataset from a `RDD`
    val rdd = sc.parallelize(seq)
    val ds3: Dataset[(Int, String, Int)] = sqlContext.createDataset(rdd)
    val ds4: Dataset[(Int, String, Int)] = rdd.toDS()

    // Creates a Dataset from a `DataFrame`
    val df = rdd.toDF("id", "name", "age")
    val ds5: Dataset[Person] = df.as[Person]

    // Selects a column
    ds5.select(expr("name").as[String]).show()

    // Filtering
    ds5.filter(_.name == "Bob").show()
    ds5.filter(person => person.age == 23).show()

    // Groups and counts the number of rows
    ds5.groupBy(_.age).count().show()
  }
}
