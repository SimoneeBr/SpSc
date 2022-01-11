package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.spsc.utils.{Commons, SparkHelper}

object italianTweets extends SparkHelper {

  //take all italian tweets, join with users so they probably are italian people, no one guarantee it

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

    println(execute(sparkSession).count())
  }

  def execute(sparkSession: SparkSession): Dataset[Row] = {
    val tweets = Commons.readTweetsFromFile(sparkSession).dropDuplicates("id")
    val users = Commons.readUsersFromFile(sparkSession).dropDuplicates("id")
    val joined = tweets.join(users, tweets("author_id") === users("id"))
    joined.filter(joined("lang") === "it").select("text")
  }

  def apiCall(): Long = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("WARN")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    execute(sparkSession).count()
  }
}
