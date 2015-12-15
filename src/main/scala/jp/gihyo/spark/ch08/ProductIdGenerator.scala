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

import scala.util.Random

// 購入ログ生成のための商品ID生成器
trait ProductIdGenerator {
  def getNextPid(recOpts: RecommendLogOptions, purchaseLog: List[Purchase]): Long
}

object ProductIdGenerator {
  // ランダムな商品IDを一つ返す
  case object RandomSelection extends ProductIdGenerator {
    // numProducts に100が設定されている場合、1から100までのLong型の整数をランダムに返す
    override def getNextPid(recOpts: RecommendLogOptions, purchaseLog: List[Purchase]): Long = {
      (Random.nextInt(recOpts.numProducts) + 1).toLong
    }
  }

  // 優先的選択に基づいて商品IDを一つ返す
  case object PreferentialAttachment extends ProductIdGenerator {
    override def getNextPid(recOpts: RecommendLogOptions, purchaseLog: List[Purchase]): Long = {
      // 商品数+購入ログ数の範囲でランダムな値を生成
      Random.nextInt(recOpts.numProducts + purchaseLog.size) + 1 match {

        // ランダム値が商品数以下の値の場合、その値を商品IDとして返す
        case index if index <= recOpts.numProducts => index.toLong

        // ランダム値が商品数を超えている場合、超過分を購入ログのインデックスとみなして、対応する商品IDを購入ログから取得する
        case index => purchaseLog(index - recOpts.numProducts - 1).pid
      }
    }
  }

  def fromString(s: String): ProductIdGenerator = s match {
    case "PreferentialAttachment" => PreferentialAttachment
    case "RandomSelection" => RandomSelection
    case _ => RandomSelection
  }

}
