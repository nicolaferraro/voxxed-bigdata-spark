package com.voxxed.bigdata.spark

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConversions._
import scala.util.Random

object KafkaSupport {

  val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka:9092",
      "key.serializer" -> classOf[StringSerializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "value.serializer" -> classOf[StringSerializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> ("spark-" + Random.nextLong()),
      // Use a random group id at each restart: this is a demo configuration
      // In production you may want to use a fixed group and checkpoints
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

  def kafkaParamsWithServer(servers: String): Map[String, Object] = kafkaParams.updated("bootstrap.servers", servers)


  private lazy val producer = {
    val producer = new KafkaProducer[String, String](kafkaParams)
    sys.addShutdownHook {
      producer.close()
    }
    producer
  }

  def send(topic: String, key: String, value: String): Unit = producer.send(new ProducerRecord[String, String](topic, key, value))

}
