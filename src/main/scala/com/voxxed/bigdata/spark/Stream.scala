package com.voxxed.bigdata.spark

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.{SparkConf, SparkContext}


object Stream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("voxxed-bigdata-spark")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val watchWindow = Minutes(10)
    val topK = 10

//    val source = ssc.socketTextStream("localhost", 9999)
//      .map(Event.fromJson)

    val source = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Seq("stars"), KafkaSupport.kafkaParams)
    ).map(record => Event.fromJson(record.value()))

    val events = source.window(watchWindow)
      .map(e => ((e.userId, e.itemId), e))
      .groupByKey()
      .flatMap({case ((user, item), events) => events.filter(e => !events.exists(e2 => e2.timestamp > e.timestamp))})
      .cache()

    val state = events
      .map(e => (e.userId, e))
      .groupByKey()
      .map({ case (user, items) => items.toList })
      .flatMap(list => {
        val prod = for(x <- list; y <- list; if (x != y)) yield (x, y)
        prod.map({ case (ev1, ev2) => ((ev1.itemId, ev2.itemId), BigDecimal(ev1.rating) - BigDecimal(ev2.rating)) })
      })
      .groupByKey()
      .mapValues(rat => (rat.size, rat.sum))
      .map({ case ((item1, item2), (size, sum)) => RatingInfo(item1, item2, size, sum) })
      .map(r => ((r.item1, r.item2), r))
      .map({ case ((item1, item2), rating) => (item1, rating) })


    val recommendations = events
      .map(event => (event.itemId, event))
      .join(state)
      .map({ case (itemId, (event, ratingInfo)) =>
        ((event.userId, ratingInfo.item2), BigDecimal(event.rating) - ratingInfo.sum / ratingInfo.count)
      })
      .groupByKey()
      .mapValues(rs => rs.sum / rs.size)
      .map({ case ((user, item), rating) => (user, (item, rating)) })


    val filteredRecommendations = events.map(e => (e.userId, e))
      .cogroup(recommendations)
      .flatMap({ case (user, (watchedMovies, suggestions)) =>
        val watched = watchedMovies.map(e => e.itemId).toSet
        suggestions.filter(s => !watched(s._1))
          .map({ case (movie, rating) => (user, (movie, rating)) })
      })

    val topByUser = filteredRecommendations
      .groupByKey()
      .mapValues(sugg => sugg.toList.sortBy({ case (movie, prediction) => -prediction }).map(_._1).take(topK))
      .map({ case (user, items) => Recommendation(user, items) })


    topByUser.foreachRDD(rdd => rdd.collect().toList.foreach(recomm => {
      // Do not use collect() in production
      KafkaSupport.send("recommendations", recomm.userId.toString, recomm.toJson)
    }))
//    topByUser.print()

    ssc.start()
    ssc.awaitTermination()
  }


}
