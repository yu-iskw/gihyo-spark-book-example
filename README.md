# 技術評論社「詳解Spark」のサンプルコード

[![License](http://img.shields.io/:license-Apache%202-red.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Build Status](https://travis-ci.org/yu-iskw/gihyo-spark-book-example.svg?branch=master)](https://travis-ci.org/yu-iskw/gihyo-spark-book-example)
[![codecov.io](https://codecov.io/github/yu-iskw/gihyo-spark-book-example/coverage.svg?branch=master)](https://codecov.io/github/yu-iskw/gihyo-spark-book-example?branch=master)

このリポジトリでは，技術評論社「詳解Spark」のサンプルコードを公開します．
サンプルコードを実行する方法は２つあります．
1. このリポジトリをビルドして `spark-submit` で指定して実行
2. `spark-shell` でこのリポジトリに対応する Spark Packages を利用

## このリポジトリをビルドして `spark-submit` で指定して実行
このソースコードを実行するには，Apache Spark 1.6 が必要です．ビルドをするためには，次のコマンドを実行します．
各 Spark アプリケーションの実行方法については，「各章の説明」を参照してください．

```shell
./build/sbt clean assembly

# ３章のサンプルコードの一つを実行
spark-submit --class jp.gihyo.spark.ch03.basic_transformation.MapExample \
    path/to/gihyo-spark-book-example_2.11-1.0.1.jar
```

### 各章の説明
各章で紹介されているサンプルコードの実行方法については，次をご覧ください．

- [３章 RDD](./ch03/README.md)
- [５章 DataFrameとSpark SQL](./ch05/README.md)
- [６章 Spark Streaming](./ch06/README.md)
- [７章 MLlib](./ch07/README.md)

## `spark-shell` でこのリポジトリに対応する Spark Packages を利用

このリポジトリに対応する Spark Packages は，[gihyo-spark-book-example](http://spark-packages.org/package/yu-iskw/gihyo-spark-book-example) で公開しています．
末尾の `1.0.1` は，このリポジトリのパッケージのバージョンです．
最新のバージョンがずれている可能性もあるので，Spark Pacakges 上の最新のバージョンにご注意ください．

```shell
$> $SPARK_HOME/bin/spark-shell --packages yu-iskw:gihyo-spark-book-example:1.0.1
```

```scala
// ３章のサンプルコードの一つを実行
scala> jp.gihyo.spark.ch03.MapExample.run(sc)
```
