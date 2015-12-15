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

class PurchaseLogGeneratorSuite extends FunSuite {

  test("generate products, users, purchaseLog") {
    val numProducts = 10
    val numUsers = 10
    val numProductsPerUser = 2
    implicit val recOpts: RecommendLogOptions = RecommendLogOptions(numProducts, numUsers, numProductsPerUser)
    val products = PurchaseLogGenerator.genProductList
    assert(products.size === numProducts)
  }

  test("generate user list") {
    val numProducts = 10
    val numUsers = 10
    val numProductsPerUser = 2
    implicit val recOpts: RecommendLogOptions = RecommendLogOptions(numProducts, numUsers, numProductsPerUser)
    val users = PurchaseLogGenerator.genUserList
    assert(users.size === numUsers)
  }

  test("generate purchaseLog with RandomSelection") {
    val numProducts = 10
    val numUsers = 10
    val numProductsPerUser = 2
    implicit val recOpts: RecommendLogOptions = RecommendLogOptions(numProducts, numUsers, numProductsPerUser)
    implicit val pidGenerator = ProductIdGenerator.fromString("RandomSelection")

    val users = PurchaseLogGenerator.genUserList
    val purchaseLog = PurchaseLogGenerator.genPurchaseLog(users)

    assert(purchaseLog.size === numUsers * numProductsPerUser)
    assert(purchaseLog.groupBy(_.uid).size === numUsers)
  }

  test("generate purchaseLog with PreferentialAttachment") {
    val numProducts = 10
    val numUsers = 10
    val numProductsPerUser = 2
    implicit val recOpts: RecommendLogOptions = RecommendLogOptions(numProducts, numUsers, numProductsPerUser)
    implicit val pidGenerator = ProductIdGenerator.fromString("PreferentialAttachment")

    val users = PurchaseLogGenerator.genUserList
    val purchaseLog = PurchaseLogGenerator.genPurchaseLog(users)

    assert(purchaseLog.size === numUsers * numProductsPerUser)
    assert(purchaseLog.groupBy(_.uid).size === numUsers)
  }
}
