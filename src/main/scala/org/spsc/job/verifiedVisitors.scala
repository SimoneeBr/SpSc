package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
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

    println(allTweetsByVerifiedVisitor(sparkSession).count()) //all tweets produced by verified users
    println(allVerifiedVisitors(sparkSession).count()) //all verified users which wrote at least one tweet in this collection
  }

  private def allTweetsByVerifiedVisitor(sparkSession: SparkSession): Dataset[Row] = {
    val commons = Commons.usersJoined(sparkSession)
    commons
      .dropDuplicates("id")
      .filter(commons("verified"))
    // println("All tweets produced by verified users: " + commons.count())
  }

  private def allVerifiedVisitors(sparkSession: SparkSession): Dataset[Row] = {
    val commons = Commons.readUsersFromFile(sparkSession)
    commons
      .dropDuplicates("id")
      .filter(commons("verified"))
    // println("All verified users: " + commons.count())
  }

  def allTweetsByVerifiedVisitorAPI(): Long = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("ERROR")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()
    allTweetsByVerifiedVisitor(sparkSession).count()
  }

  def allVerifiedVisitorsAPI(): Long = {

    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("ERROR")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    allVerifiedVisitors(sparkSession).count()
  }


}
