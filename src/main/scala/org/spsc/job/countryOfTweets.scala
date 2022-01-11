package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.spsc.utils.{Commons, SparkHelper}

import java.util
import scala.util.parsing.json.JSONObject

object countryOfTweets extends SparkHelper {

  // ALL TWEETS GROUPED BY COUNTRY ORIGIN


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
    var commons = Commons.globalQueryJoined(sparkSession)
    commons = commons.dropDuplicates("tweet_id")
      .groupBy("country")
      .count()
      .sort(desc("count")).limit(5)
    println("RESULTS\n")
    commons
  }

  def apiCall(): util.List[String] = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    import sparkSession.implicits._

    execute(sparkSession).map(row => {
      val x = row.getValuesMap(row.schema.fieldNames)
      JSONObject(x).toString()
    }).collectAsList()
  }
}
