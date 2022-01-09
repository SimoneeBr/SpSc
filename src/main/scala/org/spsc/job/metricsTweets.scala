package org.spsc.job

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, desc}
import org.spsc.job.countryOfTweets.{execute, getSparkContext}
import org.spsc.utils.{Commons, SparkHelper}

import scala.util.parsing.json.JSONObject

object metricsTweets extends SparkHelper {

  //TOP 5 OF LIKE, COMMENTS, RETWEET AND

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

  def execute(sparkSession: SparkSession):  Unit = {
    val threshold = 5
    val commons = Commons.readTweetsFromFile(sparkSession)
    val tweetMetrics = commons.select(col("id"), col("public_metrics.*"))
    val tweetMetricsFlattenDF = tweetMetrics.toDF("tweet_id", "pm_retweet_count", "pm_reply_count", "pm_like_count", "pm_quote_count")

    println("RETWEET_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_retweet_count"))
      .select("tweet_id", "pm_retweet_count")
      .limit(threshold).show()



    println("REPLY_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_reply_count"))
      .select("tweet_id", "pm_reply_count")
      .limit(threshold).show()


    println("LIKE_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_like_count"))
      .select("tweet_id", "pm_like_count")
      .limit(threshold).show()


    println("QUOTE_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_quote_count"))
      .select("tweet_id", "pm_quote_count")
      .limit(threshold).show()

  }

 def retweet_count(sparkSession: SparkSession):  Dataset[Row] = {
   val threshold = 5
   val commons = Commons.readTweetsFromFile(sparkSession)
   val tweetMetrics = commons.select(col("id"), col("public_metrics.*"))
   val tweetMetricsFlattenDF = tweetMetrics.toDF("tweet_id", "pm_retweet_count", "pm_reply_count", "pm_like_count", "pm_quote_count")
   println("RETWEET_COUNT")
   tweetMetricsFlattenDF
     .sort(desc("pm_retweet_count"))
     .select("tweet_id", "pm_retweet_count")
     .limit(threshold)
 }
  def reply_count(sparkSession: SparkSession):  Dataset[Row] = {
    val threshold = 5
    val commons = Commons.readTweetsFromFile(sparkSession)
    val tweetMetrics = commons.select(col("id"), col("public_metrics.*"))
    val tweetMetricsFlattenDF = tweetMetrics.toDF("tweet_id", "pm_retweet_count", "pm_reply_count", "pm_like_count", "pm_quote_count")
    println("REPLY_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_reply_count"))
      .select("tweet_id", "pm_reply_count")
      .limit(threshold)
  }
  def like_count(sparkSession: SparkSession):  Dataset[Row] = {
    val threshold = 5
    val commons = Commons.readTweetsFromFile(sparkSession)
    val tweetMetrics = commons.select(col("id"), col("public_metrics.*"))
    val tweetMetricsFlattenDF = tweetMetrics.toDF("tweet_id", "pm_retweet_count", "pm_reply_count", "pm_like_count", "pm_quote_count")
    println("LIKE_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_like_count"))
      .select("tweet_id", "pm_like_count")
      .limit(threshold)
  }
  def quote_count(sparkSession: SparkSession):  Dataset[Row] = {
    val threshold = 5
    val commons = Commons.readTweetsFromFile(sparkSession)
    val tweetMetrics = commons.select(col("id"), col("public_metrics.*"))
    val tweetMetricsFlattenDF = tweetMetrics.toDF("tweet_id", "pm_retweet_count", "pm_reply_count", "pm_like_count", "pm_quote_count")
    println("QUOTE_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_quote_count"))
      .select("tweet_id", "pm_quote_count")
      .limit(threshold)
  }

  def api_retweet_count(): util.List[String] = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    import sparkSession.implicits._

    retweet_count(sparkSession).map(row => {
      val x = row.getValuesMap(row.schema.fieldNames)
      JSONObject(x).toString()
    }).collectAsList()
  }

  def api_reply_count(): util.List[String] = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    import sparkSession.implicits._

    reply_count(sparkSession).map(row => {
      val x = row.getValuesMap(row.schema.fieldNames)
      JSONObject(x).toString()
    }).collectAsList()
  }

  def api_like_count(): util.List[String] = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    import sparkSession.implicits._

    like_count(sparkSession).map(row => {
      val x = row.getValuesMap(row.schema.fieldNames)
      JSONObject(x).toString()
    }).collectAsList()
  }

  def api_quote_count(): util.List[String] = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    import sparkSession.implicits._

    quote_count(sparkSession).map(row => {
      val x = row.getValuesMap(row.schema.fieldNames)
      JSONObject(x).toString()
    }).collectAsList()
  }
}
