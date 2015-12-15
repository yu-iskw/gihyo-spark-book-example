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

import org.scalatest.FunSuite

class ProductIdGeneratorSuite extends FunSuite {

  test("get next productId by RandomSelection") {
    val numProducts = 5
    val numUsers = 5
    val numProductsPerUser = 2
    implicit val recOpts: RecommendLogOptions =
      RecommendLogOptions(numProducts, numUsers, numProductsPerUser)
    val purchaseLog = List(
      Purchase(6L, 1L),
      Purchase(6L, 2L),
      Purchase(7L, 3L),
      Purchase(7L, 4L),
      Purchase(8L, 5L),
      Purchase(8L, 1L),
      Purchase(9L, 2L),
      Purchase(9L, 3L),
      Purchase(10L, 4L),
      Purchase(10L, 5L)
    )
    (1 to 10).foreach( i => {
      val pid = ProductIdGenerator.RandomSelection.getNextPid(recOpts, purchaseLog)
      assert(0 <= pid && pid <= numProducts)
    })
  }

  test("get next productId by PreferentialAttachment") {
    val numProducts = 5
    val numUsers = 5
    val numProductsPerUser = 2
    implicit val recOpts: RecommendLogOptions = RecommendLogOptions(numProducts, numUsers, numProductsPerUser)
    val purchaseLog = List(
      Purchase(6L, 1L),
      Purchase(6L, 2L),
      Purchase(7L, 3L),
      Purchase(7L, 4L),
      Purchase(8L, 5L),
      Purchase(8L, 1L),
      Purchase(9L, 2L),
      Purchase(9L, 3L),
      Purchase(10L, 4L),
      Purchase(10L, 5L)
    )
    (1 to 10).foreach( i => {
      val pid = ProductIdGenerator.PreferentialAttachment.getNextPid(recOpts, purchaseLog)
      assert(0 <= pid && pid <= numProducts)
    })
  }

  test("get ProductIdGenerator from string") {
    assert(ProductIdGenerator.RandomSelection === ProductIdGenerator.fromString("RandomSelection"))
    assert(ProductIdGenerator.PreferentialAttachment === ProductIdGenerator.fromString("PreferentialAttachment"))
    assert(ProductIdGenerator.RandomSelection === ProductIdGenerator.fromString("hoge"))
  }

}
