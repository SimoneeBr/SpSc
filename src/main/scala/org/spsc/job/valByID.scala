package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.spsc.utils.{Commons, SparkHelper}

import java.util
import scala.util.parsing.json.JSONObject

object valByID extends SparkHelper {
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

    execute(sparkSession, "1478062424024567810")
  }

  def execute(sparkSession: SparkSession, param: String): Dataset[Row] = {
    val tweet = Commons.readTweetsFromFile(sparkSession)
    val tmp = tweet.filter(tweet("id") === param)
    tmp.select("id", "text", "author_id", "created_at", "lang", "source")
  }

  def apiCall(param: String): util.List[String] = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("WARN")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()


    import sparkSession.implicits._

    execute(sparkSession, param).map(row => {
      val x = row.getValuesMap(row.schema.fieldNames)
      JSONObject(x).toString()
    }).collectAsList()
  }
}
