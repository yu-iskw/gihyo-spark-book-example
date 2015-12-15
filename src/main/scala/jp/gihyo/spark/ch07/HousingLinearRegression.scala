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

package jp.gihyo.spark.ch07

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

/**
  * House price estimation by linear regression.
  *
  * Run with
  * {{{
  * spark-submit --class jp.gihyo.spark.ch07.HousingLinearRegression \
  *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
  *     path/to/housing.data
  * }}}
  */
object HousingLinearRegression {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println(
        """
          | Usage:
          | spark-submit --class jp.gihyo.spark.ch07.HousingLinearRegression \
          |     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
          |     path/to/housing.data
        """.stripMargin)
      return
    }
    val filename = args(0)

    val sparkConf = new SparkConf().setAppName("HousingLinearRegression")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    run(sc, sqlContext, args(0))

    sc.stop()
  }

  def run(sc: SparkContext, sqlContext: SQLContext, housingFilename: String) {
    import sqlContext.implicits._

    // 1. データセットを RDD としてロードして DataFrame に変換
    val df = sc.textFile(housingFilename)
      // ホワイトスペース区切りで文字列表現されている数値を Double の配列に変換
      .map(_.trim.split("\\s+").map(_.toDouble))
      // Vector インスタンスで特徴ベクトルを表現し、LabeledPoint でラベル付きのデータにする
      .map(v => LabeledPoint(v.last, Vectors.dense(v.take(v.length - 1))))
      .toDF()

    // 2. 多項式展開の Transformer による交互作用項の追加
    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features").setOutputCol("polyFeatures")

    // 3. 線形回帰の学習器の準備
    // 数値の正規化はせず、また最適化ソルバーに L-BFGS を利用する
    // 線形回帰に入力として与える特徴ベクトルはパラメータグリッドで設定する (チューニング要素とする)
    val regressor = new LinearRegression()
      .setStandardization(false)
      .setSolver("l-bfgs")
      .setLabelCol("label")

    // 4. パラメータグリッドを構築
    val paramGrid = new ParamGridBuilder()
      .addGrid(polynomialExpansion.degree, Array(2, 3))
      .addGrid(regressor.maxIter, Array(100, 125, 150))
      .addGrid(regressor.regParam, Array(0.0, 0.01, 0.1))
      .addGrid(regressor.featuresCol, Array("features", "polyFeatures"))
      .addGrid(regressor.featuresCol, Array("polyFeatures"))
      .build()

    // 5. 交差検証による最良の予測モデルを構築
    val cvModel = new CrossValidator()
      .setEstimator(new Pipeline().setStages(Array(polynomialExpansion, regressor)))
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
      .fit(df)

    // 6. 交差検証の結果を表示

    // 各パラメータの組み合わせにおけるメトリクスの値と最良のモデルのメトリクスを表示する
    cvModel.avgMetrics.foreach(println)
    println(s"Best metrics: ${cvModel.avgMetrics.min}")

    // 最良のパラメータの組み合わせを表示する
    cvModel.bestModel.parent match {
      case pipeline: Pipeline =>
        pipeline.getStages.zipWithIndex.foreach { case (stage, index) =>
          println(s"Stage[${index + 1}]: ${stage.getClass.getSimpleName}")
          println(stage.extractParamMap())
        }
    }
  }
}

// scalastyle:on println
