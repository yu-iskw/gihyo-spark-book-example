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

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.scalatest.FunSuite

class StationSuite extends FunSuite {

  test("should be parse") {
    val line = "2,San Jose Diridon Caltrain Station,37.329732,-121.901782,27,San Jose,8/6/2013"
    val station = Station.parse(line)

    val dateFormat = new SimpleDateFormat("MM/dd/yyy")
    assert(station.id === 2)
    assert(station.name === "San Jose Diridon Caltrain Station")
    assert(station.lat === 37.329732)
    assert(station.lon === -121.901782)
    assert(station.dockcount === 27)
    assert(station.landmark === "San Jose")
    assert(station.installation === new Timestamp(dateFormat.parse("8/6/2013").getTime))
  }
}
