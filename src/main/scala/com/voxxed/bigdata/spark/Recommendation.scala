package com.voxxed.bigdata.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class Recommendation(userId: Long, items: Seq[Long]) {

  def toJson: String = Recommendation.toJson(this)

}

object Recommendation {

  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def fromJson(json: String): Recommendation = objectMapper.readValue(json, classOf[Recommendation])

  def toJson(recommendation: Recommendation): String = objectMapper.writeValueAsString(recommendation)

}
