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

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.feature.{HashingTF, StandardScaler}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

/**
  * An example of classification by spark.mllib functions.
  *
  * Run with
  * {{{
  * spark-submit --class jp.gihyo.spark.ch07.SmsSpamMLlib \
  *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
  *     path/to/SMSSpamCollection
  * }}}
  */
object SmsSpamMLlib {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println(
        """
          | Usage:
          | spark-submit --class jp.gihyo.spark.ch07.SmsSpamMLlib \
          |     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
          |     path/to/SMSSpamCollection
        """.stripMargin)
      return
    }

    val conf = new SparkConf().setAppName("SmsSpamMLlib")
    val sc = new SparkContext(conf)

    run(sc, args(0))

    sc.stop()
  }

  def run(sc: SparkContext, smsFilename: String) {
    val hashingTF = new HashingTF(100000)

    // 1. データセット (SMS Spam Collection) を RDD としてロード
    val data = sc.textFile(smsFilename)
      // 2. ラベルを含めたメッセージ本文をホワイトスペースで分割
      .map(_.split("\\s"))
      // 3. 単語の出現頻度を数え上げ、フィーチャーハッシングで特徴ベクトルを構築
      // 合わせて、文字列表現のラベルを数値に変換
      .map(v => new LabeledPoint(if (v(0) == "spam") 1.0 else 0.0, hashingTF.transform(v.drop(1))))

    // 4. 単語の出現頻度を、それぞれの単語ごとに正規化 (z 変換)
    val scaler = new StandardScaler().fit(data.map(_.features))
    val scaledData = data.map(v => v.copy(features = scaler.transform(v.features)))

    // ロジスティック回帰の正則化パラメータに 2.0 を設定
    val lr = new LogisticRegressionWithLBFGS()
    lr.optimizer.setRegParam(2.0)

    // 5. データセットを教師データと検証データに分割
    val k = 4
    val predictions = MLUtils.kFold(scaledData, k, 1).map { case (training, test) =>
      // 6. 教師データを用いて、スパムメッセージを検知する予測モデルをロジスティック回帰で構築
      val model = lr.run(training)
      // 7. 予測モデルに検証データを与えて予測値を出力
      test.map(lp => (model.predict(lp.features), lp.label))
    }

    val result = predictions.reduce((rdd1, rdd2) => rdd1.union(rdd2))

    // 8. 予測結果とラベルから、評価メトリクスを計測

    // 二値分類の評価メトリクスで AUC を計測する
    val binaryMetrics = new BinaryClassificationMetrics(result)
    println(s"AUC: ${binaryMetrics.areaUnderROC()}")

    // 多クラス分類の評価メトリクスで混同行列その他のメトリクスを計測する
    val multiclassMetrics = new MulticlassMetrics(result)
    println(s"Confusion matrix: \n${multiclassMetrics.confusionMatrix}")

    println(s"True-positive rate of 0.0=ham : ${multiclassMetrics.truePositiveRate(0.0)}")
    println(s"True-positive rate of 1.0=spam: ${multiclassMetrics.truePositiveRate(1.0)}")

    println(s"False-positive rate of 0.0=ham : ${multiclassMetrics.falsePositiveRate(0.0)}")
    println(s"False-positive rate of 1.0=spam: ${multiclassMetrics.falsePositiveRate(1.0)}")

    println(s"Precision of 0.0=ham : ${multiclassMetrics.precision(0.0)}")
    println(s"Precision of 1.0=spam: ${multiclassMetrics.precision(1.0)}")

    println(s"Recall of 0.0=ham : ${multiclassMetrics.recall(0.0)}")
    println(s"Recall of 1.0=spam: ${multiclassMetrics.recall(1.0)}")
  }
}

// scalastyle:on println
