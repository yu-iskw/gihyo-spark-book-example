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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class HaversineDistanceUDFTest {

  HaversineDistanceUDF haversineDistance;
  GenericUDF.DeferredJavaObject latA;
  GenericUDF.DeferredJavaObject lonA;
  GenericUDF.DeferredJavaObject latB;
  GenericUDF.DeferredJavaObject lonB;

  @Before
  public void setUp() {
    haversineDistance = new HaversineDistanceUDF();

    latA = new GenericUDF.DeferredJavaObject(37.329732);
    lonA = new GenericUDF.DeferredJavaObject(-121.901782);
    latB = new GenericUDF.DeferredJavaObject(37.330698);
    lonB = new GenericUDF.DeferredJavaObject(-121.888979);
  }

  @Test
  public void testValidArguments() throws HiveException {
    ObjectInspector[] objectInspectors = new ObjectInspector[4];
    objectInspectors[0] = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    objectInspectors[1] = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    objectInspectors[2] = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    objectInspectors[3] = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    haversineDistance.initialize(objectInspectors);

    GenericUDF.DeferredObject[] args = {latA, lonA, latB, lonB};
    DoubleWritable result = (DoubleWritable) haversineDistance.evaluate(args);
    Assert.assertEquals(1137.088, result.get(), 10E-4);
  }

  @Test(expected = UDFArgumentException.class)
  public void testInvalidArguments() throws UDFArgumentException {
    ObjectInspector[] objectInspectors = new ObjectInspector[3];
    objectInspectors[0] = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    objectInspectors[1] = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    objectInspectors[2] = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    haversineDistance.initialize(objectInspectors);
  }
}