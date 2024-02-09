package com.berniewalker

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object FlightDataApp {
  def main(args: Array[String]): Unit = {
    val appConfigMap = Map(
      "spark.sql.shuffle.partitions" -> "5"
    )
    val appConfig = new SparkConf()
    appConfig.setAll(appConfigMap)

    val spark =
      SparkSession
        .builder()
        .config(appConfig)
        .appName("FlightDataApplication")
        .getOrCreate()

    val flightDetails = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./data/2015-summary.csv")

    val topFiveDestinations = flightDetails
      .groupBy("DEST_COUNTRY_NAME")
      .sum("COUNT")
      .withColumnRenamed("sum(COUNT)", "count")
      .sort(desc("count"))
      .limit(5)

    flightDetails.createTempView("flight_data")

    val dfInSql = spark.sql(
      """
        |select DEST_COUNTRY_NAME, sum(count) as flight_count
        |from flight_data
        |group by DEST_COUNTRY_NAME
        |order by flight_count desc
        |limit 5
        |""".stripMargin
    )

    topFiveDestinations.explain()
    dfInSql.explain()

    topFiveDestinations.show()
    dfInSql.show()
  }

}
