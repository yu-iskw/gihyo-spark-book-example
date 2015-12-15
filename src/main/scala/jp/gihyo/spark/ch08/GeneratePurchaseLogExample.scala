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
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

/**
 * ユーザの商品購入ログを生成するサンプル
 *
 * Run with
 * {{{
 * spark-submit --class jp.gihyo.spark.ch08.GeneratePurchaseLogExample \
 *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
 *     numProducts numUsers numProductsPerUser [ RandomSelection | PreferentialAttachment ]
 * }}}
 */
object GeneratePurchaseLogExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }
    // ログレベルをWARNに設定
    Logger.getLogger("org").setLevel(Level.WARN)

    // SparkContextの生成
    val conf = new SparkConf().setAppName("GeneratePurchaseLogExample")
    val sc = new SparkContext(conf)

    // 引数から設定値を取得
    val (numProducts, numUsers, numProductsPerUser): (Int, Int, Int) =
      (args(0).toInt, args(1).toInt, args(2).toInt)
    implicit val recOpts: RecommendLogOptions =
      RecommendLogOptions(numProducts, numUsers, numProductsPerUser)
    implicit val pidGenerator = ProductIdGenerator.fromString(args(3))

    run(sc)
    sc.stop()
  }

  def run(sc: SparkContext)
      (implicit recOpts: RecommendLogOptions, pidGenerator: ProductIdGenerator): Unit = {

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

    // 購入ログを生成
    val purchaseLog = PurchaseLogGenerator.genPurchaseLog(users.collect.toList).map {
      p => Edge(p.uid, p.pid, EdgeProperty(kind = "purchase", score = 1.0))
    }
    val edges: RDD[Edge[EdgeProperty]] = sc.parallelize(purchaseLog)

    // 購入ログ20件を表示
    println("===================================")
    println("get top 20 purchase log:")
    purchaseLog.take(20).foreach(x => println(s"user${x.srcId}\tpurchased a product${x.dstId}"))

    // グラフの次数分布を確認
    println("===================================")
    println("get degree distributions:")
    // 商品IDで辺を集約
    edges.groupBy(_.dstId).map {

      // 商品ごとに購入したユーザをカウント
      case (pid, uses) => uses.size

      // ユーザ数の昇順に並び替えて、空白区切りで出力
    }.collect.toList.sorted.mkString(" ") match {
      case x => println(x)
    }
  }
}
// scalastyle:on println
