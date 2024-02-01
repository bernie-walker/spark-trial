package com.berniewalker

import org.apache.spark.sql.SparkSession

object SimpleApp {

  def main(args: Array[String]): Unit = {
    val fileName = args(0)

    val spark = SparkSession.builder().appName("Simple Application").getOrCreate()

    val logData = spark.read.textFile(fileName).cache()

    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    println(s"Lines with a: $numAs, Lines with b: $numBs")
  }

}
