package org.spsc.job

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, desc, explode}
import org.spsc.job.allVisitorsByDay.{execute, getSparkContext}
import org.spsc.job.italianTweets.{execute, getSparkContext}
import org.spsc.utils.{Commons, SparkHelper}

import scala.collection.mutable.WrappedArray
import scala.collection.mutable
import scala.util.parsing.json.JSONObject

object mostFrequentHashtag extends SparkHelper {


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

    execute(sparkSession).printSchema()
    execute(sparkSession).show()
  }

  def execute(sparkSession: SparkSession): Dataset[Row] = {

    val commons = Commons.readTweetsFromFile(sparkSession)
    // Filtered Tweet DataFrame
    val filteredTweetDF = commons.filter(commons("entities").isNotNull)
    // Geo DataFrame
    val entities = filteredTweetDF.select(col("id"), col("entities.*"))



    val entitiesFlattenDF = entities.toDF("tweet_id", "entities_url","entities_hashtags","entities_1","entities_2")
      .filter(col("entities_hashtags").isNotNull).select(explode(col("entities_hashtags")))
    val hashtags= entitiesFlattenDF.select(entitiesFlattenDF.col("col.tag"))
    val hashtagsGroupBy=hashtags.groupBy("tag").count().sort(desc("count")).limit(15)
    hashtagsGroupBy

  }

  def apiCall(): util.List[String] = {
    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

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
