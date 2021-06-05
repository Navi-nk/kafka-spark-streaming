package com.data.navi

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object KafkaSparkStream extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("kafka-stream")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "data")
    .option("startingOffsets", "earliest")
    .load()

  val dataDF = df.selectExpr("cast(value as String)", "cast(timestamp as TimeStamp)")
    .as[(String, Timestamp)]

  val eventDF = dataDF.map(r => {
    val dt = r._1.split(",")
    EventRecord(dt(0), dt(1), r._2)
  })

  val aggDF = eventDF.groupBy(
    $"hotelId",
    $"eventType",
    window($"eventTime", "30 minute"))
    .count()

  aggDF.writeStream
    .foreachBatch {
      (batchDF: DataFrame, _: Long) =>
        batchDF.orderBy($"window")
          .select(
            $"hotelId",
            $"eventType",
            date_format($"window.start", "yyyy/MM/dd hh:mm:ss").as("start"),
            date_format($"window.end", "yyyy/MM/dd hh:mm:ss").as("end"),
            $"count")
          .repartition(1)
          .write.mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("/Users/navi-mac/Desktop/workspace/trivago-kafka-spark/CSVOutput")
    }
    .outputMode("complete")
    .start()
    .awaitTermination()

}
