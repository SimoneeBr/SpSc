package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.spsc.job.italianTweets.{execute, getSparkContext}
import org.spsc.utils.SparkHelper

object run extends SparkHelper {


  def main(args: Array[String]): Unit = {
    //TODO capire come impostarlo per lasciare sempre attivo spark

    // Create SparkContext
    val sparkContext = getSparkContext()
    // Create SparkSession
    while(true){}


  }
}
