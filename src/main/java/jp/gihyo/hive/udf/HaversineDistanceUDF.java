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

package jp.gihyo.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import static java.lang.Math.*;

public class HaversineDistanceUDF extends GenericUDF {

  @Override
  public ObjectInspector initialize(ObjectInspector[] objectInspectors)
      throws UDFArgumentException {
    if (objectInspectors.length != 4) {
      throw new UDFArgumentLengthException("HaversineDistance accept exactly four arguments");
    }

    for (int i = 0; i < objectInspectors.length; i++) {
      if (objectInspectors[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentLengthException("argument " + (i + 1) + " should be primitive");
      }

      PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
          ((PrimitiveObjectInspector) objectInspectors[i]).getPrimitiveCategory();
      if (primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
        throw new UDFArgumentLengthException("argument " + (i + 1) + " should be double");
      }
    }

    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    DeferredObject latA = deferredObjects[0];
    DeferredObject lonA = deferredObjects[1];
    DeferredObject latB = deferredObjects[2];
    DeferredObject lonB = deferredObjects[3];

    Double latAValue = Double.valueOf(latA.get().toString());
    Double lonAValue = Double.valueOf(lonA.get().toString());
    Double latBValue = Double.valueOf(latB.get().toString());
    Double lonBValue = Double.valueOf(lonB.get().toString());

    double distance = calcHaversineDistance(latAValue, lonAValue, latBValue, lonBValue);
    return new DoubleWritable(distance);
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "haversine_distance()";
  }

  private double calcHaversineDistance(Double latA, Double lonA, Double latB, Double lonB) {
    double deltaLat = toRadians(latB - latA);
    double deltaLon = toRadians(lonB - lonA);
    double a = pow(
        sin(deltaLat / 2), 2) + cos(toRadians(latA)) *
        cos(toRadians(latB)) * pow(sin(deltaLon / 2),
        2);
    double greatCircleDistance = 2 * atan2(sqrt(a), sqrt(1 - a));
    return 6371000 * greatCircleDistance;
  }
}
