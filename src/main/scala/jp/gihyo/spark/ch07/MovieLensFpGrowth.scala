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

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

// scalastyle:off println

/**
  * Movie recommendation by association rule mining (FP-Growth).
  *
  * Run with
  * {{{
  * spark-submit --class jp.gihyo.spark.ch07.MovieLensFpGrowth \
  *     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
  *     path/to/ratings.dat path/to/movies.dat
  * }}}
  */
object MovieLensFpGrowth {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println(
        """
          | Usage:
          | spark-submit --class jp.gihyo.spark.ch07.MovieLensFpGrowth \
          |     path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
          |     path/to/ratings.dat path/to/movies.dat
        """.stripMargin)
      return
    }

    val sparkConf = new SparkConf().setAppName("MovieLensFpGrowth")
    val sc = new SparkContext(sparkConf)

    run(sc, args(0), args(1))

    sc.stop()
  }

  def run(sc: SparkContext, ratingsFilename: String, moviesFilename: String) {
    // 1. データセットをロードして RDD で表現
    // ユーザ::映画::評点 の文字列表現からユーザごとの映画集合 (配列表現) のデータ構造に変換する
    // (評点において 4 点以上を付けた映画のみに限定する)
    val ratings = sc.textFile(ratingsFilename)
      .map(_.split("::").map(_.toInt))
      .filter(_(2) > 3)
      .map(v => (v(0), v(1)))
      .groupByKey()
      .map(t => t._2.toArray)

    // 2. 映画タイトルをロードして Map で表現
    // 推薦結果の確認用に、映画 ID からタイトル名を参照できるようにする
    val movies = sc.textFile(moviesFilename)
      .map(_.split("::").take(2))
      .map(v => (v(0).toInt, v(1)))
      .collectAsMap()

    // 3. FP-Growth による映画の頻出パターンを抽出
    // 支持度が 0.1 以上のアイテムの組み合わせを列挙する
    val model = new FPGrowth()
      .setMinSupport(0.1)
      .run(ratings)

    // 4. 頻出パターンからアソシエーションルールを生成
    // 信頼度が 0.2 以上となるアソシエーションルールのみを抽出する
    val rules = model.generateAssociationRules(0.2)

    // 5. 結果の表示
    rules.collect().foreach { rule =>
      println(
        rule.antecedent.map(movies.get(_).get).mkString("[", ",", "]")
          + " => " + rule.consequent.map(movies.get(_).get).mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }
}

// scalastyle:on println
