#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import warnings

import math
from datetime import date, datetime as dt

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD
from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

class Station(object):
    """Station class"""

    def __init__(self, station_id, name, lat, lon, dockcount, landmark, installation):
        self.station_id   = station_id
        self.name         = name
        self.lat          = lat
        self.lon          = lon
        self.dockcount    = dockcount
        self.landmark     = landmark
        self.installation = installation

    @classmethod
    def is_head(cls, line):
        if "station_id" in line:
            return True
        else:
            return False

    @classmethod
    def parse(cls, line):
        elms = line.split(',')
        station_id = int(elms[0])
        name = elms[1]
        lat = float(elms[2])
        lon = float(elms[3])
        dockcount = int(elms[4])
        landmark = elms[5]
        parsed = dt.strptime(elms[6], "%m/%d/%Y")
        installation = date(parsed.year, parsed.month, parsed.day)
        return (station_id, name, lat, lon, dockcount, landmark, installation)


class Trip(object):

    def __init__(self, trip_id, duration,
                 startDate, startStation, startTerminal,
                 endDate, endStation, endTerminal,
                 bikeNum, subscriberType, zipcode):
        self.trip_id
        self.duration
        self.startDate
        self.startStation
        self.startTerminal
        self.endDate
        self.endStation
        self.endTerminal
        self.bikeNum
        self.subscriberType
        self.zipcode

    @classmethod
    def is_head(cls, line):
        if "Trip ID" in line:
            return True
        else:
            return False

    @classmethod
    def parse(cls, line):
        elms = line.split(',')
        trip_id = int(elms[0])
        duration = int(elms[1])
        startDate = dt.strptime(elms[2], "%m/%d/%Y %H:%M")
        startStation = elms[3]
        startTerminal = int(elms[4])
        endDate = dt.strptime(elms[5], "%m/%d/%Y %H:%M")
        endStation = elms[6]
        endTerminal = int(elms[7])
        bikeNum = int(elms[8])
        subscriberType = elms[9]
        zipcode = elms[10]
        return (trip_id, duration, startDate, startStation, startTerminal,
                endDate, endStation, endTerminal, bikeNum, subscriberType, zipcode)

class DataFrameExample(object):

    @classmethod
    def main(cls, params):
        if len(params) != 2:
            print("Usage: DataFrameExample <station_csv> <trip_csv>")
            exit(-1)
        station_path = params[0]
        trip_path = params[1]

        conf = SparkConf()
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        example = DataFrameExample()
        example.run(sc, sqlContext, station_path, trip_path)

    def run(self, sc, sqlContext, station_path, trip_path):
        stationDF = self.read_station(sc, station_path)
        tripDF = self.read_trip(sc, trip_path)
        stationDF.cache()
        tripDF.cache()

        # Converts to RDD
        rdd = stationDF.rdd

        # Shows schema
        stationDF.printSchema()

        # Selects columns
        stationDF.select(stationDF.station_id).show(5, False)

        # Counts the number of records
        print(stationDF.count())

        # Filters records
        print(stationDF.filter(stationDF.landmark == "San Jose").count())

        # Sorts by a column
        stationDF.orderBy("dockcount", ascending=False).show(5, False)

        # Uses a function
        stationDF.select(stationDF.name, length("name"), expr("length(name)")).show(5, False)

        # Group By
        stationDF.groupBy(stationDF.dockcount).count().show(5, False)

        # Aggregation
        stationDF.groupBy(stationDF.landmark).agg({"dockcount": "sum"}).show()

        # Joins with DataFrames
        start = stationDF.alias("start") \
            .toDF("start_station_id", "start_name", "start_lat", "start_lon",
                 "start_dockcount", "start_landmark", "start_installation")
        end = stationDF.alias("end") \
            .toDF("end_station_id", "end_name", "end_lat", "end_lon",
                 "end_dockcount", "end_landmark", "end_installation")
        joinedTripDF = tripDF.join(start, tripDF.startTerminal == start.start_station_id, "inner") \
            .join(end, tripDF.endTerminal == end.end_station_id, "inner")
        joinedTripDF.printSchema()

        # Define an UDF
        def haversine_distance(pointA, pointB):
            deltaLat = math.radians(pointB[0] - pointA[0])
            deltaLon = math.radians(pointB[1] - pointA[1])
            a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.radians(pointA[0])) * \
                math.cos(math.radians(pointB[0])) * math.pow(math.sin(deltaLon / 2), 2)
            greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            return 6371000 * greatCircleDistance
        distance = udf(lambda latA, lonA, latB, lonB: haversine_distance((latA, lonA), (latB, lonB)),
                       DoubleType())
        joinedTripDF.select("startTerminal", 
                            "endTerminal", 
                            distance("start_lat", "start_lon", "end_lat", "end_lon")).show(5, False)

        # Writes a DataFrame
        joinedTripDF.write.mode("overwrite").format("json").save("./path/to/json/")

        # Reads a DataFrame
        loadedDF = sqlContext.read.format("json").load("./path/to/json/")
        loadedDF.printSchema()

        # Spark SQL
        tripDF.registerTempTable("trip")
        df = sqlContext \
            .sql("SELECT count(trip_id) AS count FROM trip WHERE to_date(startDate) = '2015-01-01'")
        df.show()

        # User an UDF in Spark SQL
        joinedTripDF.registerTempTable("joined_trip")
        sqlContext.registerFunction("distance",
                                    lambda latA, lonA, latB, lonB: \
                                    haversine_distance((latA, lonA), (latB, lonB)))
        result = sqlContext.sql("SELECT " +
            "startTerminal, " +
            "endTerminal, " +
            "distance(start_lat, start_lon, end_lat, end_lon) AS dist " +
            "FROM joined_trip " +
            "WHERE startTerminal != endTerminal")
        result.show(5, False)


    def read_station(self, sc, station_path):
        return sc.textFile(station_path, 2) \
            .filter(lambda line: not Station.is_head(line)) \
            .map(lambda line: Station.parse(line)) \
            .toDF().toDF("station_id", "name", "lat", "lon",
                         "dockcount", "landmark", "installation")

    def read_trip(self, sc, trip_path):
        return sc.textFile(trip_path, 2) \
            .filter(lambda line: not Trip.is_head(line)) \
            .map(lambda line: Trip.parse(line)) \
            .toDF().toDF( "trip_id", "duration",
                         "startDate", "startStation", "startTerminal",
                         "endDate", "endStation", "endTerminal",
                         "bikeNum", "subscriberType", "zipcode")


if __name__ == '__main__':
    DataFrameExample.main(sys.argv[1:])
