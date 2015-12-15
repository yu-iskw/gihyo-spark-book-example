# ５章：DataFrameとSpark SQL

「５章DataFrameとSparkSQL」のサンプルコードの解説をします．サンプルコードを実行するためには，本書内で解説したデータが必要です．次の URL から September 2014 - August 2015 のデータをダウンロードしてください．

- [http://www.bayareabikeshare.com/open-data](http://www.bayareabikeshare.com/open-data)

## DataFrame の基本操作のサンプルコード

「5.3.2 DataFrame の生成」および「5.3.3 DataFrame の基本操作」に対応するサンプルコードは次になります．
- [src/main/scala/jp/gihyo/spark/ch05/BasicDataFrameExample.scala](https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/src/main/scala/jp/gihyo/spark/ch05/BasicDataFrameExample.scala)

サンプルコードを実行するためには，次のコマンドを実行します．
```shell
spark-submit --class jp.gihyo.spark.ch05.BasicDataFrameExample \
    path/to/gihyo-spark-book-example_2.10-1.0.1.jar
    201508_station_data.csv 201508_trip_data.csv
```

## DataFrame の欠損値処理のサンプルコード
「5.3.4 DataFrame の欠損値の扱い方」に対応するサンプルコードは次になります．
- [src/main/scala/jp/gihyo/spark/ch05/DataFrameNaFunctionExample.scala.scala](https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/src/main/scala/jp/gihyo/spark/ch05/DataFrameNaFunctionExample.scala)

## JDBC 経由でデータを取得するサンプルコード
「5.5.2 JDBC による連携」の解説に対応するサンプルコードは次になります．
MySQLのテーブルのデータを取得するためには，JDBC ドライバをパスに通しておく必要があります．
- [src/main/scala/jp/gihyo/spark/ch05/JdbcExample.scala](https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/src/main/scala/jp/gihyo/spark/ch05/JdbcExample.scala)

サンプルコードを実行するためには，MySQL のテーブルを用意する必要があります．テーブルを作成する SQL は次になります．
- [ch05/ch05-mysql-example.sql](https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/ch05/ch05-mysql-example.sql)

サンプルコードを実行するためには，次のコマンドを実行します．
```shell
spark-submit --class jp.gihyo.spark.ch05.JdbcExample \
    path/to/gihyo-spark-book-example_2.10-1.0.1.jar \
    "jdbc:mysql://localhost/gihyo_spark" "MYSQL_USER" "MYSQL_PASS"
```

MySQLに接続するための情報をコマンドの引数として渡します．
- 第１引数：JDBC 接続する MySQL のURL
- 第２引数：接続するユーザ名
- 第３引数：接続するためのパスワード

## BAY AERA BikeShare の分析のコード
「5.7 DataFrame API を利用した BAY AREA BikeShare の分析」の解説に対応するサンプルコードは次になります．

- [src/main/scala/jp/gihyo/spark/ch05/BikeShareAnalysisExample.scala](https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/src/main/scala/jp/gihyo/spark/ch05/BikeShareAnalysisExample.scala)
- [R/pkg/R/ch05-visualize-map.R](https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/R/pkg/R/ch05-visualize-map.R)

サンプルコードを実行するためには，次のコマンドを実行します．
SparkR コードの実行がうまくいかない場合は，`${SPARK_HOME}/bin/sparkR` コマンドで SparkR のプロンプトを立ち上げて直接実行してください．
```shell
# scala コードの実行
spark-submit --class jp.gihyo.spark.ch05.BikeShareAnalysisExample \
    path/to/gihyo-spark-book-example_2.10-1.0.1.jar
    201508_station_data.csv 201508_trip_data.csv

# SparkR コードの実行
spark-submit R/pkg/R/ch05-visualize-map.R
```
