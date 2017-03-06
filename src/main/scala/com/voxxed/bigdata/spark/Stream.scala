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

    val watchWindow = Minutes(5)
    val topK = 10

    ssc.checkpoint("/tmp/checkpoint")

    def updateFunction(newValues: Seq[RatingInfo], runningCount: Option[RatingInfo]): Option[RatingInfo] = {
      val curCount = runningCount.map(r => r.count).getOrElse(0L)
      val curSum = runningCount.map(r => r.sum).getOrElse(BigDecimal(0))
      val newCount = newValues.map(r => r.count).fold(curCount)(_ + _)
      val newSum = newValues.map(r => r.sum).fold(curSum)(_ + _)

      val result = runningCount.orElse(newValues.headOption).map(r => r.copy(count = newCount, sum = newSum))
      result
    }

//        val events = ssc.socketTextStream("localhost", 9999)
//          .map(Event.fromJson)

    val events = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Seq("stars"), KafkaSupport.kafkaParams)
    ).map(record => Event.fromJson(record.value()))

    val state = events
      .map(e => (e.userId, e))
      .groupByKey()
      .map({ case (user, items) => items.toList.sortBy(_.timestamp) })
      .flatMap(list => list.zip(list.drop(1)).map({ case (ev1, ev2) => (ev1.itemId, ev2.itemId, BigDecimal(ev1.rating) - BigDecimal(ev2.rating)) }))
      .map({ case (item1, item2, diff) => ((item1, item2), diff) })
      .groupByKey()
      .mapValues(rat => (rat.size, rat.sum))
      .map({ case ((item1, item2), (size, sum)) => RatingInfo(item1, item2, size, sum) })
      .flatMap(rating => Seq(rating, rating.copy(item1 = rating.item2, item2 = rating.item1, sum = -rating.sum)))
      .map(r => ((r.item1, r.item2), r))
      .updateStateByKey(updateFunction)
      .map({ case ((item1, item2), rating) => (item1, rating) })


    val latestEvents = events.window(watchWindow)
      .map(e => ((e.userId, e.itemId), e))
      .groupByKey()
      .flatMap({ case (userItem, evts) => evts.filter(e => !evts.exists(e2 => e2.timestamp > e.timestamp)) })

    val recommendations = latestEvents
      .map(event => (event.itemId, event))
      .join(state)
      .map({ case (itemId, (event, ratingInfo)) =>
        ((event.userId, ratingInfo.item2), BigDecimal(event.rating) - ratingInfo.sum / ratingInfo.count)
      })
      .groupByKey()
      .mapValues(rs => rs.sum / rs.size)
      .map({ case ((user, item), rating) => (user, (item, rating)) })


    val filteredRecommendations = latestEvents.map(e => (e.userId, e))
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

//    topByUser.foreachRDD(rdd => rdd.foreach(recomm => KafkaSupport.send("recommendations", recomm.userId.toString, recomm.toJson)))
    topByUser.foreachRDD(rdd => rdd.foreach(recomm => println(recomm.toJson)))

    ssc.start()
    ssc.awaitTermination()
  }


}
