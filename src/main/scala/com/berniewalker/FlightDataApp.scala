package com.berniewalker

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{
  Dataset,
  RelationalGroupedDataset,
  Row,
  SparkSession
}
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}

//case class FlightDetail(
//    destCountryName: String,
//    originCountryName: String,
//    count: Int
//)

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

    topFiveDestinations.explain("extended")
//    topFiveDestinations.explain("codegen")
//    topFiveDestinations.explain("cost")
//    topFiveDestinations.explain("formatted")

    topFiveDestinations.show()
  }

}
