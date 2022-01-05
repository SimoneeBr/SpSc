package org.spsc.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait SparkHelper extends Logging {

  def getSparkContext(): SparkContext = {
    getSparkSession().sparkContext
  }

  def getSparkSession(): SparkSession = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("BigDataProject")
    val session = SparkSession.builder().config(conf).getOrCreate()

    session
  }
}
