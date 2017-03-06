package com.voxxed.bigdata.spark

case class RatingInfo(item1: Long, item2: Long, count: Long, sum: BigDecimal) {

  def add(r: RatingInfo): RatingInfo = RatingInfo(item1, item2, count + r.count, sum + r.sum)

}
