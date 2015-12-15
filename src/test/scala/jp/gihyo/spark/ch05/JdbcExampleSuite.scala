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

import java.sql.DriverManager
import java.util.Properties

import jp.gihyo.spark.{SparkFunSuite, TestSparkContext}
import org.scalatest.BeforeAndAfter

class JdbcExampleSuite extends SparkFunSuite with TestSparkContext with BeforeAndAfter {

  val user = "testUser"
  val pass = "testPass"
  val url = "jdbc:h2:mem:testdb;MODE=MySQL"
  val urlWithUserAndPass = s"jdbc:h2:mem:testdb;user=${user}};password=${pass}"
  var conn: java.sql.Connection = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    Class.forName("org.h2.Driver")
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("CREATE SCHEMA gihyo_spark").executeUpdate()
    conn.prepareStatement(
      """
        |CREATE TABLE gihyo_spark.person (
        |  id INTEGER NOT NULL,
        |  name TEXT(32) NOT NULL,
        |  age INTEGER NOT NULL
        |)
      """.stripMargin.replaceAll("\n", " ")
    ).executeUpdate()
    conn.prepareStatement("INSERT INTO gihyo_spark.person VALUES (1, 'fred', 23)").executeUpdate()
    conn.prepareStatement("INSERT INTO gihyo_spark.person VALUES (2, 'mary', 22)").executeUpdate()
    conn.prepareStatement("INSERT INTO gihyo_spark.person VALUES (3, 'bob', 23)").executeUpdate()
    conn.prepareStatement("INSERT INTO gihyo_spark.person VALUES (4, 'ann', 22)").executeUpdate()
    conn.commit()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    conn.close()
  }

  test("run") {
    JdbcExample.run(sc, sqlContext, url, user, pass)
  }
}

