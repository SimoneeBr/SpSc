package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.spsc.utils.{Commons, SparkHelper}

object verifiedVisitors extends SparkHelper {
  // COUNT TWEETS ONLY BY VERIFIED USERS
  // COUNT USERS ONLY VERIFIED

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

    allTweetsByVerifiedVisitor(sparkSession) //all tweets produced by verified users
    allVerifiedVisitors(sparkSession) //all verified users which wrote at least one tweet in this collection
  }

  def allTweetsByVerifiedVisitor(sparkSession: SparkSession): Unit = {
    var commons = Commons.usersJoined(sparkSession)
    commons = commons
      .dropDuplicates("id")
      .filter(commons("verified"))
    println("All tweets produced by verified users: " + commons.count())
  }

  def allVerifiedVisitors(sparkSession: SparkSession): Unit = {
    var commons = Commons.readUsersFromFile(sparkSession)
    commons = commons
      .dropDuplicates("id")
      .filter(commons("verified"))
    println("All verified users: " + commons.count())
  }


}
