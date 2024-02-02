package com.berniewalker

import org.apache.spark.sql.SparkSession

object SimpleApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SimpleApplication").getOrCreate()

    val numbersDf = spark.range(20).toDF("number")

    val evenNumbers = numbersDf.where((numbersDf("number") % 2).equalTo(0))

    evenNumbers.show()
  }


}
