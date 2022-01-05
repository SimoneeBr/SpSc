package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, desc}
import org.spsc.utils.{Commons, SparkHelper}

object allVisitorsByDay extends SparkHelper {

  // ALL TWEETS CREATED GROUPED BY DAY

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
    val allJoined = Commons.globalQueryJoined(sparkSession);
    var filteredUAE = allJoined.filter((allJoined("country") === "Emirati Arabi Uniti") || allJoined("country") === "AE")
    filteredUAE = filteredUAE
      .select(filteredUAE.col("*"), date_format(filteredUAE("created_at"), "dd/MM/yyyy").as("formatted_data"))
      .drop("created_at")
      .dropDuplicates("tweet_id")
      .dropDuplicates(Array("author_id", "formatted_data"))
      .groupBy("formatted_data").count()
      .sort(desc("count"))

    println("RESULTS\n")
    filteredUAE.show(true)
  }
}
