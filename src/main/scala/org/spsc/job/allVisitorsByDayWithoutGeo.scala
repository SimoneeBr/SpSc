package org.spsc.job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, date_format, desc}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.spsc.utils.{Commons, SparkHelper}
import java.util

import scala.util.parsing.json.JSONObject

object allVisitorsByDayWithoutGeo extends SparkHelper {

// ALL TWEETS CREATED GROUPED BY DAY

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

  def execute(sparkSession: SparkSession): Dataset[Row] = {
  val filteredUAE = Commons.readTweetsFromFile(sparkSession)

   val group= filteredUAE.select(date_format(col("created_at"), "dd/MM/yyyy").as("formatted_data"))
    .drop("created_at")
    .groupBy("formatted_data").count()
    .sort(desc("count"))

  println("RESULTS\n")
    group.show(true)
    group
}

  def apiCall(): util.List[String] = {
  // Create SparkContext
  val sparkContext = getSparkContext()
  sparkContext.setLogLevel("WARN")

  // Create SparkSession
  val sparkSession = SparkSession
  .builder()
  .getOrCreate()

  import sparkSession.implicits._

  execute(sparkSession).map(row => {
  val x = row.getValuesMap(row.schema.fieldNames)
  JSONObject(x).toString()
}).collectAsList()
}


}
