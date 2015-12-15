# ３章：RDD

「３章 RDD」のサンプルコードの解説です。

## 「3.4.1 基本的な変換」のサンプル

各メソッドはそれぞれ下記のコマンドで実行可能です。

```shell
# map
spark-submit --class jp.gihyo.spark.ch03.basic_transformation.MapExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# filter
spark-submit --class jp.gihyo.spark.ch03.basic_transformation.FilterExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# flatMap
spark-submit --class jp.gihyo.spark.ch03.basic_transformation.FlatMapExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# mapPartitions
spark-submit --class jp.gihyo.spark.ch03.basic_transformation.MapPartitionsExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# distinct
spark-submit --class jp.gihyo.spark.ch03.basic_transformation.DistinctExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# sample
spark-submit --class jp.gihyo.spark.ch03.basic_transformation.SampleExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar
```

## 「3.4.2 RDDを引数にとる変換」のサンプル

各メソッドはそれぞれ下記のコマンドで実行可能です。

```shell
# zip
spark-submit --class jp.gihyo.spark.ch03.basic_transformation.ZipExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# 集合演算
spark-submit --class jp.gihyo.spark.ch03.basic_transformation.SetOperationsExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar
```

## 「3.5.1 基本のアクション」のサンプル

各メソッドはそれぞれ下記のコマンドで実行可能です。

```shell
# count
spark-submit --class jp.gihyo.spark.ch03.basic_action.CountExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# take
spark-submit --class jp.gihyo.spark.ch03.basic_action.TakeExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# first
spark-submit --class jp.gihyo.spark.ch03.basic_action.FirstExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# top, takeOrdered
spark-submit --class jp.gihyo.spark.ch03.basic_action.OrderExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar
```

## 「3.5.2 集約のアクション」のサンプル

各メソッドはそれぞれ下記のコマンドで実行可能です。

```shell
# reduce
spark-submit --class jp.gihyo.spark.ch03.basic_action.ReduceExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# fold
spark-submit --class jp.gihyo.spark.ch03.basic_action.FoldExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# aggregate
spark-submit --class jp.gihyo.spark.ch03.basic_action.AggregateExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar
```

## 「3.5.3 算術演算のアクション」のサンプル

各メソッドはそれぞれ下記のコマンドで実行可能です。

```shell
spark-submit --class jp.gihyo.spark.ch03.basic_action.StatsExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar
```

## 「3.6.1 ペアRDD（キー・バリュー形式のRDD）の操作」のサンプル

各メソッドはそれぞれ下記のコマンドで実行可能です。

```shell
# mapValues
spark-submit --class jp.gihyo.spark.ch03.pairrdd_transformation.MapValuesExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# reduceByKey
spark-submit --class jp.gihyo.spark.ch03.pairrdd_transformation.ReduceByKeyExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# foldByKey
spark-submit --class jp.gihyo.spark.ch03.pairrdd_transformation.FoldByKeyExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# aggregateByKey
spark-submit --class jp.gihyo.spark.ch03.pairrdd_transformation.AggregateByKeyExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# combineByKey
spark-submit --class jp.gihyo.spark.ch03.pairrdd_transformation.CombineByKeyExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# groupByKey
spark-submit --class jp.gihyo.spark.ch03.pairrdd_transformation.GroupByKeyExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# sortByKey
spark-submit --class jp.gihyo.spark.ch03.pairrdd_transformation.SortByKeyExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# collectAsMap
spark-submit --class jp.gihyo.spark.ch03.pairrdd_action.CollectAsMapExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar
```

## 「3.6.2 RDDの結合」のサンプル

各メソッドはそれぞれ下記のコマンドで実行可能です。

```shell
# join
spark-submit --class jp.gihyo.spark.ch03.pairrdd_transformation.JoinExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar

# cogroup
spark-submit --class jp.gihyo.spark.ch03.pairrdd_transformation.CoGroupExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar
```

## 「3.6.3 RDDの永続化」のサンプル

下記のサンプルは１つの引数をとります。

```shell
spark-submit --class jp.gihyo.spark.ch03.persistence.PersistExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar \
    $SPARK_HOME/README.md
```

* 第１引数　ファイルパス

## 「3.6.4 共有変数」のサンプル

下記のサンプルは１つの引数をとります。

```shell
spark-submit --class jp.gihyo.spark.ch03.shared_variable.WordCountExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar \
    $SPARK_HOME/README.md
```

* 第１引数　ファイルパス

## 「3.7 パーティション」のサンプル

各メソッドはそれぞれ下記のコマンドで実行可能です。

```shell
# repartition, coalesce
spark-submit --class jp.gihyo.spark.ch03.partition.PartitionExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar /usr/local/opt/spark-1.6.0-bin-hadoop2.6/README.md

# カスタムパーティショナ
spark-submit --class jp.gihyo.spark.ch03.partition.CustomPartitionerExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar /usr/local/opt/spark-1.6.0-bin-hadoop2.6/README.md
```
