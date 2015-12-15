# ７章：MLlib

「７章 MLlib」のサンプルコードの解説です。

## データのダウンロード

サンプルコードの入力データとして与えるファイルを事前にダウンロードしておく必要があります。
本ディレクトリに含まれる `download-ch07-data.sh` を実行することで、必要なファイルをまとめてダウンロードできます。

## スパムメッセージの検出のサンプルコード
 
「7.3.1 スパムメッセージの検出」のサンプルコードは次になります。

- [src/main/scala/jp/gihyo/spark/ch07/SmsSpamMLlib.scala](src/main/scala/jp/gihyo/spark/ch07/SmsSpamMLlib.scala)

サンプルコードの実行は次のとおりです。


```shell
spark-submit --class jp.gihyo.spark.ch07.SmsSpamMLlib \
    path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
    path/to/SMSSpamCollection
```

## きのこの可食・有毒の識別のサンプルコード

「7.3.2 きのこの可食・有毒の識別」のサンプルコードは次になります。

- [src/main/scala/jp/gihyo/spark/ch07/MushroomDecisionTree.scala](src/main/scala/jp/gihyo/spark/ch07/MushroomDecisionTree.scala)

サンプルコードの実行は次のとおりです。

```shell
spark-submit --class jp.gihyo.spark.ch07.MushroomDecisionTree \
    path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
    path/to/agaricus-lepiota.data
```

## 住宅価格の予測のサンプルコード

「7.3.3 住宅価格の予測」のサンプルコードは次になります。

- [src/main/scala/jp/gihyo/spark/ch07/HousingLinearRegression.scala](src/main/scala/jp/gihyo/spark/ch07/HousingLinearRegression.scala)

サンプルコードの実行は次のとおりです。

```shell
spark-submit --class jp.gihyo.spark.ch07.HousingLinearRegression \
    path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
    path/to/housing.data
```

## 好みの映画のレコメンドのサンプルコード

「7.3.4 好みの映画のレコメンド」のサンプルコードは次になります。

- [src/main/scala/jp/gihyo/spark/ch07/MovieLensFpGrowth.scala](src/main/scala/jp/gihyo/spark/ch07/MovieLensFpGrowth.scala)

サンプルコードの実行は次のとおりです。

```shell
spark-submit --class jp.gihyo.spark.ch07.MovieLensFpGrowth \
    path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
    path/to/ratings.dat path/to/movies.dat
```
