package com.voxxed.bigdata.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class Event(userId: Long, itemId: Long, rating: Int, timestamp: Long) {

  def toJson = Event.toJson(this)

}

object Event {

  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def fromJson(json: String): Event = objectMapper.readValue(json, classOf[Event])

  def toJson(event: Event): String = objectMapper.writeValueAsString(event)

}
