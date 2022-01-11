package org.spsc.job

import org.spsc.utils.SparkHelper

object run extends SparkHelper {


  def main(args: Array[String]): Unit = {
    //TODO capire come impostarlo per lasciare sempre attivo spark

    // Create SparkContext
    val sparkContext = getSparkContext()
    // Create SparkSession
    while (true) {}


  }
}
