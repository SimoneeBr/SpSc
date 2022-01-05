package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.spsc.utils.{Commons, SparkHelper}

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

  def execute(sparkSession: SparkSession): Unit = {
    var commons = Commons.usersJoined(sparkSession)
    commons = commons
      .filter(commons("location").isNotNull)
      .dropDuplicates("id")
      .groupBy("location")
      .count()
      .sort(desc("count"))
    println("RESULTS\n")
    commons.show(false) //FIXME country with arab name not showing properly
  }
}
