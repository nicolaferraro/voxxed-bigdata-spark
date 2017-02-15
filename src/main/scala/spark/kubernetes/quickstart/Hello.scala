package spark.kubernetes.quickstart

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object Hello {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Hello")
    val sc = new SparkContext(conf)

    val result = sc.parallelize(1 to 100).reduce(_ + _)
    println(result)
  }

}
