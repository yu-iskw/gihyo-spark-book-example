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

// scalastyle:off println
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.functions._

/**
  * An example code of the basic usage of DataFrame
  *
  * Run with
  * {{{
  * spark-submit --class jp.gihyo.spark.ch05.BasicDataFrameExample \
  *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar
  *     201508_station_data.csv 201508_trip_data.csv
  * }}}
  */
object BasicDataFrameExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      new IllegalArgumentException("Invalid arguments")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("BasicDataFrameExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val stationPath = args(0)
    val tripPath = args(1)
    run(sc, sqlContext, stationPath, tripPath)

    sc.stop()
  }

  def run(
    sc: SparkContext,
    sqlContext: SQLContext,
    stationPath: String,
    tripPath: String): Unit = {
    import sqlContext.implicits._

    // Creates a DataFrame for the station data with a case class
    val stationRDD: RDD[(Int, String, Double, Double, Int, String, java.sql.Date)] =
      sc.textFile(stationPath).
        filter(line => !line.contains("station_id")).
        map { line =>
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
          (id, name, lat, lon, dockcount, landmark, installation)
        }
    val stationDF = stationRDD.
      map { case (id, name, lat, lon, dockcount, landmark, installation) =>
        Station(id, name, lat, lon, dockcount, landmark, installation)
      }.toDF().cache()

    // Converts a DataFrame to RDD
    val rdd = stationDF.rdd
    // Uses getters of `Row`
    rdd.foreach { row =>
      val id = row.getInt(0)
      val name = row.getString(1)
      println(s"(id, name) = ($id, $name)")
    }
    rdd.foreach(row => println(s"${row.get(1)}"))

    // Shows the schema of the station DataFrame
    stationDF.printSchema()

    val localStation = stationDF.collect()

    // Selects and shows columns
    stationDF.select('id, $"name", stationDF("landmark"), col("dockcount")).show()
    stationDF.select('id, $"name", lit("STATION"), expr("id + 1")).show(5, false)

    // Counts the number of rows
    stationDF.count()
    stationDF.select('landmark).distinct().count()

    // Makes an alias of a column
    stationDF.select('name as "station_name").show(3, false)

    // Filters with a condition
    stationDF.filter('landmark === "San Jose").count()

    // Control flows
    stationDF.select('dockcount,
      when('dockcount >= 20, 2)
        .when('dockcount >= 15, 1)
        .otherwise(0)).show(3)


    // Sorts a DataFrame by columns or expressions
    stationDF.select('name, 'dockcount).orderBy('dockcount).show(5, false)
    stationDF.select('name, 'dockcount).orderBy('dockcount.desc).show(5, false)
    stationDF.select('name, 'lat, 'lon).orderBy('lat, 'lon).show(5, false)
    stationDF.select('name).orderBy(length('name)).show(5)

    // Uses a build-in function
    stationDF.select('name, length('name)).show(3, false)

    // Aggregation and grouping
    stationDF.agg(sum('dockcount)).show()
    stationDF.groupBy('landmark).count.show()
    stationDF.groupBy('landmark).agg(sum('dockcount)).show()
    stationDF.groupBy('landmark).agg(avg('lat), avg('lon)).show()

    // Create a DataFrame for the trip data
    val tripDF = sc.textFile(tripPath).
      filter(line => !line.contains("Trip ID")).
      map(_.split(",")).
      filter(_.size == 11).
      map { elms =>
        val dateFormat = new SimpleDateFormat("MM/dd/yyy HH:mm")

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
      }.toDF().cache()

    tripDF.printSchema()

    // Joins the trip DataFrame with the station DataFrame
    var start = stationDF.as("start")
    var end = stationDF.as("end")
    start.columns.foreach { col => start = start.withColumnRenamed(col, s"start_${col}") }
    end.columns.foreach { col => end = end.withColumnRenamed(col, s"end_${col}") }
    val joinedTripDF = tripDF.
      join(start, tripDF("startTerminal") === start("start_id"), "inner").
      join(end, tripDF("endTerminal") === end("end_id"), "inner").cache()
    joinedTripDF.select('start_id, 'end_id).show(3)

    // Defines and uses an UDF
    val distance = udf((latA: Double, lonA: Double, latB: Double, lonB: Double) =>
      haversineDistance((latA, lonA), (latB, lonB)))
    joinedTripDF.select(
      'startStation,
      'endStation,
      distance('start_lat, 'start_lon, 'end_lat, 'end_lon) as "dist"
    ).filter('dist > 0.0).show(5, false)

    // Writes a DataFrame as JSON files
    tripDF.write.mode(SaveMode.Overwrite).format("json").save("./path/to/json/")

    // Loads a DataFrame from JSON files
    val df = sqlContext.read.format("json").load("./path/to/json/")

    // Operates a DataFrame with Spark SQL
    tripDF.registerTempTable("trip")
    val numRides = sqlContext.
      sql("SELECT count(id) AS count FROM trip WHERE to_date(startDate) = '2015-01-01'")
    numRides.show(1, false)

    // Registers an UDF on Spark SQL
    sqlContext.udf.register("distance",
      (lat1: Double, lon1: Double, lat2: Double, lon2: Double) =>
        haversineDistance((lat1, lon1), (lat2, lon2)))
    joinedTripDF.registerTempTable("joined_trip")
    val result = sqlContext.
      sql("SELECT " +
        "startTerminal, " +
        "endTerminal, " +
        "distance(start_lat, start_lon, end_lat, end_lon) AS dist " +
        "FROM joined_trip " +
        "WHERE startTerminal != endTerminal")
    result.show(3)

    // Unpersists DataFrames
    stationDF.unpersist()
    tripDF.unpersist()
    joinedTripDF.unpersist()
  }

  /**
    * Calculates a Haversine distance between two points by their latitudes and longitudes
    * SEE ALSO: https://en.wikipedia.org/wiki/Haversine_formula
    *
    * @param pointA Tuple2 of latitude and longitude
    * @param pointB Tuple2 of latitude and longitude
    * @return a Haversine distance
    */
  def haversineDistance(pointA: (Double, Double), pointB: (Double, Double)): Double = {
    val deltaLat = math.toRadians(pointB._1 - pointA._1)
    val deltaLon = math.toRadians(pointB._2 - pointA._2)
    val a = math.pow(
      math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(pointA._1)) *
      math.cos(math.toRadians(pointB._1)) * math.pow(math.sin(deltaLon / 2), 2)
    val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    6371000 * greatCircleDistance
  }
}

// scalastyle:on println
