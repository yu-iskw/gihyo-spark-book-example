# CONTRIBUTING

## ビルド
```shell
make assembly
```

## テスト
```shell
# 全体のテスト
make test

# Scala のテスト
make test-scala
```

## コーディングスタイルチェック
```shell
# 全体のチェック
make lint

# Scala のコードチェック
make lint-scala

# R のコードチェック
make lint-r
```

## NOTE
このパッケージは `spark-package` コマンドを利用して生成しました．[Spark Packages](http://spark-packages.org/) 上で公開をしますが，公開や更新をする権限は @yu-iskw のみが持ちます．
```shell
spark-package init --scala --java --python --R -n yu-iskw/gihyo-spark-book-example -o gihyo-spark-book-example
```
