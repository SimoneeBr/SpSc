package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
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

    execute(sparkSession)
  }

  def execute(sparkSession: SparkSession): Unit = {
    val tweets = Commons.readTweetsFromFile(sparkSession).dropDuplicates("id")
    val users = Commons.readUsersFromFile(sparkSession).dropDuplicates("id")
    var joined = tweets.join(users, tweets("author_id") === users("id"))
    joined = joined.filter(joined("lang") === "it").select("text")
    joined.show(true)
    println(joined.count())

  }
}
