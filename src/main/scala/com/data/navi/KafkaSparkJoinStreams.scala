package com.data.navi

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class EventRecord(hotelId: String, eventType: String, eventTime: Timestamp)

object KafkaSparkJoinStreams extends App {
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
    .option("subscribe", "data, locations")
    .option("startingOffsets", "earliest")
    .load()

  val dataDF = df.filter($"topic" === "data")
    .selectExpr("cast(value as String) as id", "cast(timestamp as TimeStamp) as timestamp")
    .as[(String, Timestamp)]

  val locationDF = df.filter($"topic" === "locations")
    .selectExpr("cast(key as String) as id", "cast(value as String) as location")

  val eventDF = dataDF.map(r => {
    val dt = r._1.split(",")
    EventRecord(dt(0), dt(1), r._2)
  })

  val joinedDF = eventDF.as("d1").join(locationDF.as("d2"), $"d1.hotelId" === $"d2.id")
    .select($"d2.location", $"d1.eventType", $"d1.eventTime")

  joinedDF.writeStream
    .foreachBatch {
      (batchDF: DataFrame, _: Long) =>
        batchDF.groupBy($"location", $"eventType", window($"eventTime", "30 minute"))
          .count()
          .orderBy($"window")
          .select($"location", $"eventType", date_format($"window.start", "yyyy/MM/dd hh:mm:ss"), date_format($"window.end", "yyyy/MM/dd hh:mm:ss"), $"count")
          .repartition(1)
          .write.mode(SaveMode.Overwrite)
          .csv("/Users/navi-mac/Desktop/workspace/trivago-kafka-spark/CSVOutput1")
    }
    .outputMode(OutputMode.Append())
    .start()
    .awaitTermination()

  /*val aggDF = joinedDF.withWatermark("eventTime", "1 day")
    .groupBy($"location", $"eventType", window($"eventTime", "30 minute"))
    .count()

  aggDF.writeStream
    .foreachBatch {
      (batchDF: DataFrame, _: Long) =>
        batchDF.orderBy($"window")
          .select($"location", $"eventType", date_format($"window.start", "yyyy/MM/dd hh:mm:ss"), date_format($"window.end", "yyyy/MM/dd hh:mm:ss"), $"count")
          .repartition(1)
          .write.mode(SaveMode.Overwrite)
          .csv("/Users/navi-mac/Desktop/workspace/trivago-kafka-spark/CSVOutput1")
    }
    .outputMode(OutputMode.Append())
    .start()
    .awaitTermination()*/

}

