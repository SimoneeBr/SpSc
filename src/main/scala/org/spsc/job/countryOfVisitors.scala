package org.spsc.job

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.desc
import org.spsc.job.countryOfTweets.{execute, getSparkContext}
import org.spsc.utils.{Commons, SparkHelper}

import scala.util.parsing.json.JSONObject

object countryOfVisitors extends SparkHelper {

  // ALL USERS GROUPED BY COUNTRY

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
    var commons = Commons.usersJoined(sparkSession)
    commons = commons
      .filter(commons("location").isNotNull)
      .dropDuplicates("id")
      .groupBy("location")
      .count()
      .sort(desc("count"))
    println("RESULTS\n")
    commons //FIXME country with arab name not showing properly
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
