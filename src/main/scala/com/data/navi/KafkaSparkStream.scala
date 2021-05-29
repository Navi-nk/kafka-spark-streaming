package com.data.navi

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//case class EventRecord(hotelId: String, eventType: String, eventTime: Timestamp)
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
    .option("startingOffsets" , "earliest")
    .load()
  val ndf = df.selectExpr( "cast(value as String)", "cast(timestamp as TimeStamp)")
    .as[(String, Timestamp)]

  val eventDF = ndf.map( r => {
    val dt = r._1.split(",")
    EventRecord(dt(0), dt(1), r._2)
  })

  import spark.implicits._

 // val h = eventDF.select($"hotelId").distinct()
 // val h = eventDF.select(date_format($"eventTime", "dd/MM/yyyy hh:mm:ss")).distinct()
  val h = eventDF.select($"hotelId")
  h.writeStream.format("console").outputMode("append").option("numRows", 30).option("truncate", false).start()
    .awaitTermination()

  /*val aggDF = eventDF.groupBy($"hotelId", $"eventType", window($"eventTime", "30 minute"))
    .count()

  aggDF.writeStream
    .foreachBatch {
      (batchDF: DataFrame, _: Long) =>
        batchDF.orderBy($"window")
          .select($"hotelId", $"eventType", date_format($"window.start", "yyyy/MM/dd hh:mm:ss"), date_format($"window.end", "yyyy/MM/dd hh:mm:ss"), $"count")
          .repartition(1)
          .write.mode(SaveMode.Overwrite)
          .csv("/Users/navi-mac/Desktop/workspace/trivago-kafka-spark/CSVOutput")
    }
    .outputMode("complete")
    .start()
    .awaitTermination()*/

}
