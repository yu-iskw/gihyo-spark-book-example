# 利用するライブラリの読み込み
library(ggmap)
library(grid)

# 駐輪ステーション情報を CSV ファイルから読み込む
stations <- read.csv("./data/201508_station_data.csv")
sf_stations <- subset(stations, landmark == "San Francisco")

# Scala で書き込んだデータを読み込む
trip_counts <- read.df(sqlContext, "./trip.counts.parquet", "parquet")
# R の data.frame に変換して，上位 50 行を抽出
l_trip_counts <- collect(trip_counts)
trips.top50 <-
  head(l_trip_counts[order(l_trip_counts$count, decreasing = TRUE), ], 50)

# 地図の中心点の緯度経度を計算
center <- colMeans(sf_stations[, c("lon", "lat")])
# ggmap で Google マップの描画レイヤーを取得
map <- get_googlemap(center = center, scale = 2, zoom = 14, maptype = "roadmap")
# 地図上に駐輪ステーションの位置を点で描画
gmap <- ggmap(map) +
  geom_point(
      data = sf_stations[, c("lon", "lat")],
      mapping = aes(color = "red", size = 3)
    ) + theme(legend.position = "none")
# 各利用開始点と利用終了点を矢印で描画
for (i in 1:(nrow(trips.top50) - 1)) {
  trip <- trips.top50[i, ]
  lons <- c(trip$start_lon, trip$end_lon)
  lats <- c(trip$start_lat, trip$end_lat)
  size <- trip$count / max(trips.top50[, "count"])

  path <- data.frame(lon = lons, lat = lats)
  gmap <- gmap +
    geom_path(data = path, arrow = arrow(), size = 3 * size)
}

# PNG ファイルに画像を保存
ggsave(gmap, filename = "trip-between-stations-top50.png")
