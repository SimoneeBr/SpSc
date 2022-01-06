package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}
import org.spsc.utils.{Commons, SparkHelper}

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

  def execute(sparkSession: SparkSession): Unit = {
    val threshold = 5
    val commons = Commons.readTweetsFromFile(sparkSession)
    val tweetMetrics = commons.select(col("id"), col("public_metrics.*"))
    val tweetMetricsFlattenDF = tweetMetrics.toDF("tweet_id", "pm_retweet_count", "pm_reply_count", "pm_like_count", "pm_quote_count")

    println("RETWEET_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_retweet_count"))
      .select("tweet_id", "pm_retweet_count")
      .limit(threshold)
      .show(truncate = false)

    println("REPLY_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_reply_count"))
      .select("tweet_id", "pm_reply_count")
      .limit(threshold)
      .show(truncate = false)

    println("LIKE_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_like_count"))
      .select("tweet_id", "pm_like_count")
      .limit(threshold)
      .show(truncate = false)

    println("QUOTE_COUNT")
    tweetMetricsFlattenDF
      .sort(desc("pm_quote_count"))
      .select("tweet_id", "pm_quote_count")
      .limit(threshold)
      .show(truncate = false)
  }
}
