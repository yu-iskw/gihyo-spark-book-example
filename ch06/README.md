Spark-book ch06 Readme.md

# 6章:Spark Streaming
6章:Spark Streamingのサンプルコードの解説を行います。

##Spark Streamingのサンプルコードの動かし方
サンプルコードはそれぞれ下記のコマンドで実行可能です。

###SparkStreamingのサンプル

このサンプルプログラムは引数として、socketTextStreamの接続を開くアドレスとポートをとります。

```gihyo_6_2_1_Sample
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_2_1_Sample target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

##6.5の各メソッドのサンプル

###Cogroup

下記のサンプルは４つの引数をとります。

```Cogroupのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_Cogroup target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999 localhost 9998
```
* 第１引数　1つ目の接続先アドレス
* 第２引数　1つ目の接続先ポート
* 第３引数　２つ目の接続先アドレス　
* 第４引数　2つ目の接続先ポート

###Count
下記のサンプルは２つの引数をとります。

```Countのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_Count target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```
* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###countByValue

下記のサンプルは２つの引数をとります。

```countByValueのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_countByValue target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###countByValueAndWindow
下記のサンプルは３つの引数をとります。

```countByValueAndWindowのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_countByValueAndWindow target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999 /tmp/to/checkpoint
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート
* 第３引数　チェックポイントディレクトリ

###countByWindow

下記のサンプルは３つの引数をとります。

```countByWindowのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_countByWindow target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999 /tmp/to/checkpoint
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート
* 第３引数　チェックポイントディレクトリ

###filter
下記のサンプルは２つの引数をとります。


```filterのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_Filter target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###flatMap

下記のサンプルは２つの引数をとります。

```flatMapのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_flatMap target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###join

下記のサンプルは４つの引数をとります。

```joinのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_Join target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999 localhost 9998
```

* 第１引数　１つ目の接続先アドレス
* 第２引数　１つ目の接続先ポート
* 第３引数　２つ目の接続先アドレス
* 第４引数　２つ目の接続先ポート

###map
下記のサンプルは２つの引数をとります

```mapのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_Map target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###reduce
下記のサンプルは２つの引数をとります

```reduceのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_Reduce target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###reduceByKey
下記のサンプルは２つの引数をとります

```reduceByKeyのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_reduceByKey target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###reduceByKeyAndWindow
下記のサンプルは2つの引数をとります

```reduceByKeyAndWindowのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_reduceByKeyAndWindow target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###reduceByKeyAndWindow_efficient
下記のサンプルは３つの引数をとります

```reduceByKeyAndWindow_efficientのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_reduceByKeyAndWindow_efficient target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999 /tmp/to/checkpoint
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###union
下記のサンプルは３つの引数をとります。
また、このサンプルはApache Kafka,Zookeeperを利用している為、
それぞれのミドルウェアの実行環境が必要です

```unionのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_Union target/scala-2.10/gihyo-spark-book-example-assembly-1.0.1.jar localhost:2128 cGroup test
```

* 第１引数　zookeeperQuorum
* 第２引数　consumerGroup名
* 第３引数　データを取得するtopics

###windowのサンプル
下記のサンプルは２つの引数をとります。

```windowのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_Window target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

###updateStateByKey
下記のサンプルは３つの引数をとります

```updateStateByKeyのサンプル
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_updateStateByKey target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar localhost 9999 /tmp/to/checkpoint/updateStateByKey
```

* 第１引数　接続先アドレス
* 第２引数　接続先ポート

##6.8 Spark Streamingの例
下記のそれぞれのサンプルは
Apache Kafka, Zookeeper
TwitterAPIを叩く為のconsumerKey consumerSeecretKey AccessToken AccessTokenSecretをあらかじめ取得しておく必要があります。


##kafkaを使ったサンプルプログラム

下記のサンプルは４つの引数をとります。
また、下記のサンプルはApache Kafkaが実行されている必要があります。

```kafkaのサンプルプログラム
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_KafkaStream target/scala-2.10/gihyo-spark-book-example-assembly-1.0.1.jar localhost:9092 test /tmp/to/checkpoint/kafkastream /tmp/to/saveData
```

* 第１引数　kafkaのBrokerのList
* 第２引数　データを取得するtopic
* 第３引数　チェックポイントディレクトリ
* 第４引数　データの保存先ディレクトリ

##twitterを使ったサンプルプログラム
下記のサンプルは７つの引数をとります
また、下記のサンプルを実行する前にあらかじめTwitterAPIを叩く為の各Key,Tokenを取得しておく必要があります。

```twitterのサンプルプログラム
spark-submit --class jp.gihyo.spark.ch06.gihyo_6_3_TwitterStream target/scala-2.11/gihyo-spark-book-example-assembly-1.0.1.jar <consumerKey> <consumerSecret> <accessToken> <accessTokenSecret> /tmp/to/checkpoint/twitterstream /tmp/to/tagdir /tmp/to/worddir
```

* 第１引数　consumerKey
* 第２引数　consumerSecret
* 第３引数　accessToken
* 第４引数　accessTokenSecret
* 第５引数　チェックポイントディレクトリ
* 第６引数　集計後タグの出力先ディレクトリ
* 第７引数　集計後ワードの出力先ディレクトリ
