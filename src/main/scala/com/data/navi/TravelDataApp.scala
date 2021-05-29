package com.data.navi

/*import java.time.{Instant, LocalDate, ZoneId}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class EventRecord(id: String, eventType: String, eventTime: LocalDate)

object TravelDataApp extends App {
  val conf = new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("NetworkWordCount")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "spark-test2",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val ssc = new StreamingContext(conf, Seconds(2))
  ssc.sparkContext.setLogLevel("ERROR")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](Array("data"), kafkaParams)
  )
  var rows = stream.map(record => {
    val data = record.value().split(",")
    EventRecord(data(0), data(1), Instant.ofEpochMilli(record.timestamp()).atZone(ZoneId.systemDefault()).toLocalDate)
  })
  rows.foreachRDD( r => r.collect().sortWith((a,c) => a.eventTime.compareTo(c.eventTime) < 0).foreach(println))
  ssc.start()
  ssc.awaitTermination()
  //df.show()

}*/

