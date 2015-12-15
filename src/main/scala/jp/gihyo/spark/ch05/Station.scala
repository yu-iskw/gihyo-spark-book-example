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

case class Station(id: Int, name: String, lat: Double, lon: Double,
    dockcount: Int, landmark: String, installation: java.sql.Date)

object Station {

  def parse(line: String): Station = {
    val dateFormat = new SimpleDateFormat("MM/dd/yyy")

    val elms = line.split(",")
    val id = elms(0).toInt
    val name = elms(1)
    val lat = elms(2).toDouble
    val lon = elms(3).toDouble
    val dockcount = elms(4).toInt
    val landmark = elms(5)
    val parsedInstallation = dateFormat.parse(elms(6))
    val installation = new java.sql.Date(parsedInstallation.getTime)
    Station(id, name, lat, lon, dockcount, landmark, installation)
  }
}

