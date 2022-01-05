package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, explode}
import org.spsc.utils.{Commons, SparkHelper}

object mostFrequentHashtag extends SparkHelper {

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

  def execute(sparkSession: SparkSession): Unit = {
    //TODO finish this query
    val commons = Commons.readTweetsFromFile(sparkSession)
    val df2 = commons.select(col("id"), explode(col("entities.hashtags")))
    df2.groupBy("id").agg(collect_list("col").alias("hashtags")).sort("hashtags")
  }
}
