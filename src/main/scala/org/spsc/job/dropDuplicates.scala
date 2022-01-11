package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.spsc.utils.{Commons, SparkHelper}

object dropDuplicates extends SparkHelper {

  def main(args: Array[String]): Unit = {
    //Added to hide all info and warnings of Spark
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    execute(sparkSession)
  }

  def execute(sparkSession: SparkSession): Dataset[Row] = {
    var twe = Commons.readPlacesFromFile(sparkSession)
    twe = twe.dropDuplicates("id")
    println(twe.count())
    twe.coalesce(1).write.json("./src/main/resources/completeAllPlacesIncludes.json")
    twe
  }
}
