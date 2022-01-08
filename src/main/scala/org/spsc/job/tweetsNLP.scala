package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.spsc.job.sentiment.SentimentAnalyzer
import org.spsc.utils.{Commons, SparkHelper}

object tweetsNLP extends SparkHelper {
  //TODO TEST ON CRISTIAN PC

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

    execute(sparkSession)
  }

  def execute(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    var tweets = Commons.readTweetsFromFile(sparkSession)
    tweets = tweets.filter(tweets("lang") === "en")
    tweets.select("text").limit(10).map(row => SentimentAnalyzer.extractSentiments(row.getString(0))).foreach(x=>println(x))

  }

}