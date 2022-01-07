package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.spsc.utils.{Commons, SparkHelper}

object allVisitors extends SparkHelper {

  def main(Args: Array[String]): Unit = {

    //Added to hide all info and warnings of Spark
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    println(execute(sparkSession).count())
  }


  private def execute(sparkSession: SparkSession): Dataset[Row] = {
    val allJoined = Commons.globalQueryJoined(sparkSession)
    allJoined
      .filter((allJoined("country") === "Emirati Arabi Uniti") || allJoined("country") === "AE")
      .dropDuplicates("author_id")
  }

  def apiCall(): Long = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("ERROR")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    execute(sparkSession).count()
  }


}
