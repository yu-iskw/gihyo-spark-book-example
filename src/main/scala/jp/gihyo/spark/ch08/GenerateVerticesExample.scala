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

package jp.gihyo.spark.ch08

// scalastyle:off println
import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
 * 商品リストとユーザリストのRDDを生成するサンプル
 *
 * Run with
 * {{{
 * spark-submit --class jp.gihyo.spark.ch08.GenerateVerticesExample \
 *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
 *     numProducts numUsers
 * }}}
 */
object GenerateVerticesExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }
    // ログレベルをWARNに設定
    Logger.getLogger("org").setLevel(Level.WARN)

    // SparkContextの生成
    val conf = new SparkConf().setAppName("GenerateVerticesExample")
    val sc = new SparkContext(conf)

    // 引数から設定値を取得
    val (numProducts, numUsers): (Int, Int) = (args(0).toInt, args(1).toInt)
    implicit val recOpts: RecommendLogOptions = RecommendLogOptions(numProducts, numUsers)

    run(sc)
    sc.stop()
  }

  def run(sc: SparkContext)
         (implicit recOpts: RecommendLogOptions)
  : Unit = {

    // 商品リスト、ユーザリストのRDDを生成
    val products: RDD[VertexProperty] = sc.parallelize(PurchaseLogGenerator.genProductList)
    val users: RDD[VertexProperty] = sc.parallelize(PurchaseLogGenerator.genUserList)

    // 商品リスト20件を表示
    println("===================================")
    println("get top 20 products:")
    products.take(20).foreach(x => println(s"id: ${x.id},\ttype: ${x.kind},\tname: ${x.name}"))

    // ユーザリスト20件を表示
    println("===================================")
    println("get top 20 users:")
    users.take(20).foreach(x => println(s"id: ${x.id},\ttype: ${x.kind},\tname: ${x.name}"))

  }
}
// scalastyle:on println
