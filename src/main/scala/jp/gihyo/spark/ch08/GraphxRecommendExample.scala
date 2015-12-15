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
import org.apache.spark.graphx.{EdgeDirection, Graph, Edge}
import org.apache.spark.rdd.RDD

/**
 * An example code of recommendation by using GraphX
 *
 * Run with
 * {{{
 * spark-submit --class jp.gihyo.spark.ch08.GraphxRecommendExample \
 *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
 *     numProducts numUsers numProductsPerUser [ RandomSelection | PreferentialAttachment ]
 * }}}
 */
object GraphxRecommendExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }
    // ログレベルをWARNに設定
    Logger.getLogger("org").setLevel(Level.WARN)

    // SparkContextの生成
    val conf = new SparkConf().setAppName("GraphxRecommendExample")
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

    // 商品、ユーザのリストを生成
    val products: List[VertexProperty] = PurchaseLogGenerator.genProductList
    val users: List[VertexProperty] = PurchaseLogGenerator.genUserList

    // 商品リスト20件を表示
    println("===================================")
    println("get top 20 products:")
    products.take(20).foreach(x => println(s"id: ${x.id},\ttype: ${x.kind},\tname: ${x.name}"))

    // ユーザリスト20件を表示
    println("===================================")
    println("get top 20 users:")
    users.take(20).foreach(x => println(
      s"id: ${x.id},\ttype: ${x.kind},\tname: ${x.name}"
    ))

    val vertices: RDD[(Long, VertexProperty)] =
      sc.parallelize((users ++ products).map(v => (v.id, v)), numSlices = 10).cache()

    // 購入ログを生成
    val purchaseLog = PurchaseLogGenerator.genPurchaseLog(users).map {
      p => Edge(p.uid, p.pid, EdgeProperty(kind = "purchase", score = 1.0))
    }
    val edges: RDD[Edge[EdgeProperty]] = sc.parallelize(purchaseLog)

    // 購入ログ20件を表示
    println("===================================")
    println("get top 20 purchase log:")
    purchaseLog.take(20).foreach(x => println(s"user${x.srcId}\tpurchased a product${x.dstId}"))

    // グラフの作成
    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertices, edges).cache()

    // レコメンド対象となる商品ID
    val targetProductId = 1L

    // レコメンドのリストを計算
    val recommends = genRecommendEdges(graph, targetProductId)

    // レコメンドのリストを表示
    println("===================================")
    println("get top 20 recommends:")
    recommends.take(20).foreach { x =>
      println(s"product${x.srcId}\thas a recommended product${x.dstId}\twith score ${x.attr.score}")
    }

  }

  // 商品間のつながりの強さをあらわす recommendEdges を生成する関数
  private def genRecommendEdges(graph: Graph[VertexProperty, EdgeProperty], targetId: Long)
      (implicit recOpts: RecommendLogOptions): RDD[Edge[(EdgeProperty)]] = {

    // 入力されたユーザと商品の関係をあらわすグラフに対して、 Pregel を適用
    // 前処理として、引数で指定された頂点のプロパティ値を1として、
    // それ以外の頂点のプロパティ値を0とする
    val recommends = graph.mapVertices((id, v) => if (id == targetId) 1 else 0 ).pregel(
      initialMsg = 0,     // 初期メッセージの設定
      maxIterations = 2,  // 最大イテレーション数の設定
      activeDirection = EdgeDirection.Either    // sendMsg を実行する辺の条件
    )(
      // メッセージを受信した頂点の処理
      vprog = (id, dist, newDist) => math.max(dist, newDist),

      // 頂点間のメッセージを送信する処理
      sendMsg = triplet => {
        if (triplet.srcAttr > 0) Iterator((triplet.dstId, triplet.srcAttr))
        else if (triplet.dstAttr > 0) Iterator((triplet.srcId, triplet.dstAttr))
        else Iterator.empty
      },

      // 複数のメッセージをマージする処理
      mergeMsg = (a, b) => a + b

      // 計算されたスコアが0より大きい頂点のみを抽出
    ).subgraph(vpred = (id, v) => v > 0)

    // 引数で指定された頂点のプロパティ値（次数に一致）を取得
    val degree = recommends.vertices.filter(v => v._1 == targetId).first()._2

    val recommendEdges = recommends.vertices.collect {
      // 商品ノードかつ引数で指定された頂点以外のノードを抽出
      case (dstId, d) if dstId <= recOpts.numProducts && dstId != targetId =>
        // 各商品ノードについて、引数で指定された商品を購入した人のうち、
        // その商品も購入している割合を計算
        Edge(targetId, dstId, EdgeProperty(kind = "recommend", score = d.toDouble / degree))
    }

    recommendEdges
  }

}
// scalastyle:on println
