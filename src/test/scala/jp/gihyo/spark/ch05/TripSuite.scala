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

import org.scalatest.FunSuite

class TripSuite extends FunSuite {

  test("should be parsed") {
    val line = "911926,566,8/31/2015 8:20,Harry Bridges Plaza (Ferry Building)," +
      "50,8/31/2015 8:30,Post at Kearny,47,566,Subscriber,95442"
    val trip = Trip.parse(line)
    assert(trip.id === 911926)
    assert(trip.duration === 566)
    assert(trip.zipcode === "95442")
  }
}
