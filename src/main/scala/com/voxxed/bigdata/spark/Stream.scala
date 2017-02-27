package com.voxxed.bigdata.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object Stream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Hello")
    val sc = new SparkContext(conf)
    val streamingContext = new StreamingContext(sc, Seconds(10))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("stars")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key(), record.value())).foreachRDD(rdd => rdd.foreach(println))

    streamingContext.start()
    streamingContext.awaitTermination()
    sc.stop()
  }

}
