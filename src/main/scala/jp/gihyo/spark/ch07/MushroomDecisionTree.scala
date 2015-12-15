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
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

/**
  * An example of classification by spark.ml features.
  *
  * Run with
  * {{{
  * spark-submit --class jp.gihyo.spark.ch07.MushroomDecisionTree \
  *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
  *     path/to/agaricus-lepiota.data
  * }}}
  */
object MushroomDecisionTree {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println(
        """
          | Usage:
          | spark-submit --class jp.gihyo.spark.ch07.MushroomDecisionTree \
          |     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
          |     path/to/agaricus-lepiota.data
        """.stripMargin)
      return
    }
    val sparkConf = new SparkConf().setAppName("Mushroom")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    run(sc, sqlContext, args(0))

    sc.stop()
  }

  def run(sc: SparkContext, sqlContext: SQLContext, mushroomFilename: String) {
    import sqlContext.implicits._

    // 以下は DataFrame のカラム名として利用 (veil-type は除く)
    val featureNames = Seq("_edible", "cap-shape", "cap-surface", "cap-color", "bruises?", "odor",
      "gill-attachment", "gill-spacing", "gill-size", "gill-color", "stalk-shape", "stalk-root",
      "stalk-surface-above-ring", "stalk-surface-below-ring", "stalk-color-above-ring",
      "stalk-color-below-ring", "veil-color", "ring-number", "ring-type",
      "spore-print-color", "population", "habitat")

    // 1. データセットをロードしてタプルの RDD として保持
    // (veil-type に相当する特徴量 v(16) はタプルに含めない)
    val rdd = sc.textFile(mushroomFilename)
      .map(line => line.split(","))
      .map(v => (v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7), v(8), v(9), v(10), v(11), v(12),
        v(13), v(14), v(15), v(17), v(18), v(19), v(20), v(21), v(22)))

    // 2. RDD から DataFrame に変換
    val _df = rdd.toDF(featureNames: _*)

    // 3. 文字列表現されたラベル ("e", "p") を数値に変換
    val df = new StringIndexerModel(Array("e", "p"))
      .setInputCol("_edible").setOutputCol("edible")
      .transform(_df).drop("_edible").drop("veil-type")

    // 4. R のモデル式で特徴選択モデルを構築
    val formula = new RFormula()
      .setFeaturesCol("features").setLabelCol("label")
      .setFormula("edible ~ .")
      .fit(df)

    // 5. 決定木の学習器を準備
    // 木の深さ (maxDepth) のパラメータのみ設定する
    val decisionTree = new DecisionTreeClassifier()
      .setFeaturesCol("features").setLabelCol("edible")
      .setMaxDepth(4)

    // 6. 特徴選択から学習までのパイプラインを構築
    val pipeline = new Pipeline().setStages(Array(formula, decisionTree))

    // 7. データセットの分割 (学習用と検証用)
    val trainingAndTest = df.randomSplit(Array(0.5, 0.5))

    // 8. パイプライン処理による決定木の予測モデル構築
    val pipelineModel = pipeline.fit(trainingAndTest(0))

    // 9. 予測モデルを用いて検証用データに対して予測
    val prediction = pipelineModel.transform(trainingAndTest(1))

    // 10. 予測結果から評価メトリクス (AUC) を計算
    val auc = new BinaryClassificationEvaluator()
      .evaluate(prediction)

    println(auc)
  }
}

// scalastyle:on println
