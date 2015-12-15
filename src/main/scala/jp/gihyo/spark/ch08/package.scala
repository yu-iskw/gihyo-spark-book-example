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

package jp.gihyo.spark

package object ch08 {

  /**
   * 頂点プロパティの型定義
   *
   * @param id 頂点ID
   * @param kind 頂点のエンティティ種類 (user, product)
   * @param name エンティティの名前
   */
  case class VertexProperty(val id: Long, val kind: String, val name: String)

  /**
   * 辺プロパティの型定義
   *
   * @param kind 辺の種類 (purchase, recommend)
   * @param score 辺の重み
   */
  case class EdgeProperty(val kind: String, val score: Double)

  /**
   * ユーザの商品購入アクションを表すクラス
   *
   * @param uid ユーザエンティティの頂点ID
   * @param pid 商品エンティティの頂点ID
   */
  case class Purchase(val uid: Long, val pid: Long)

  /**
   * ユーザの商品購入ログを生成するための設定
   *
   * @param numProducts 商品数
   * @param numUsers ユーザ数
   * @param numProductsPerUser 1ユーザあたりの商品購入数
   */
  case class RecommendLogOptions(
    numProducts: Int = 10,
    numUsers: Int = 10,
    numProductsPerUser: Int = 2
  )
}
