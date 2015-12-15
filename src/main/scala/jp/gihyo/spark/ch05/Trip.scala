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

import java.text.SimpleDateFormat

case class Trip(id: Int, duration: Int,
  startDate: java.sql.Timestamp, startStation: String, startTerminal: Int,
  endDate: java.sql.Timestamp, endStation: String, endTerminal: Int,
  bikeNum: Int, subscriberType: String, zipcode: String)

object Trip {

  def parse(line: String): Trip = {
    val dateFormat = new SimpleDateFormat("MM/dd/yyy HH:mm")
    val elms = line.split(",")

    val id = elms(0).toInt
    val duration = elms(1).toInt
    val startDate = new java.sql.Timestamp(dateFormat.parse(elms(2)).getTime)
    val startStation = elms(3)
    val startTerminal = elms(4).toInt
    val endDate = new java.sql.Timestamp(dateFormat.parse(elms(5)).getTime)
    val endStation = elms(6)
    val endTerminal = elms(7).toInt
    val bikeNum = elms(8).toInt
    val subscriberType = elms(9)
    val zipcode = elms(10)
    Trip(id, duration,
      startDate, startStation, startTerminal,
      endDate, endStation, endTerminal,
      bikeNum, subscriberType, zipcode)
  }
}
