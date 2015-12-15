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

import scala.annotation.tailrec

object PurchaseLogGenerator {

  // 商品ノードのデータを作成する関数
  def genProductList(implicit recOpts: RecommendLogOptions): List[VertexProperty] =
    (1 to recOpts.numProducts).map {
      id => VertexProperty(id.toLong, "product", s"product${id}")
    }.toList

  // ユーザノードのデータを作成する関数
  def genUserList(implicit recOpts: RecommendLogOptions): List[VertexProperty] =
    (1 to recOpts.numUsers).map {
      // ユーザノードの頂点IDは、商品ノードのIDとかぶらないよう、商品数で底上げする
      id => VertexProperty((recOpts.numProducts + id).toLong, "user", s"user${id}")
    }.toList


  // 引数で受け取ったユーザのリストに対して、 genUserPurchaseLog を再帰的に適用する関数
  @tailrec
  def genPurchaseLog(users: List[VertexProperty], purchaseLog: List[Purchase] = Nil)
    (implicit recOpts: RecommendLogOptions, pidGenerator: ProductIdGenerator): List[Purchase] = {
    users match {
      case Nil => purchaseLog.reverse
      case _ => genPurchaseLog(users.tail, genUserPurchaseLog(users.head, purchaseLog))
    }
  }

  /**
   * 引数で受け取ったユーザに対する購入ログを生成する関数
   * products: Set[Long] に再帰的に商品IDを追加し、 products の要素数が
   * numProductsPerUser に達するまで繰り返す
   * purchaseLog には、すでに他のユーザに対して作成された購入ログが格納されており、
   * この購入ログに、新しくいまのユーザの購入ログを追加したものを返り値とする
   */
  @tailrec
  private def genUserPurchaseLog(
      user: VertexProperty,
      purchaseLog: List[Purchase],
      products: Set[Long] = Set()
    )(implicit recOpts: RecommendLogOptions, pidGenerator: ProductIdGenerator): List[Purchase] = {

    products.size match {
      case recOpts.numProductsPerUser =>
        products.map(pid => Purchase(user.id, pid)).toList ++ purchaseLog
      case _ =>
        genUserPurchaseLog(
          user,
          purchaseLog,
          products + pidGenerator.getNextPid(recOpts, purchaseLog))
    }
  }

}
