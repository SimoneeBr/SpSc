package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, desc}
import org.spsc.job.sentiment.SentimentAnalyzer
import org.spsc.utils.{Commons, SparkHelper}

import scala.util.parsing.json.JSONObject

object tweetsNLP extends SparkHelper {
  //TODO TEST ON CRISTIAN PC

  def main(Args: Array[String]): Unit = {

    //Added to hide all info and warnings of Spark
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()
    import sparkSession.implicits._
    execute(sparkSession).groupBy("value").count().map(row => {
      val x = row.getValuesMap(row.schema.fieldNames)
      JSONObject(x).toString()
    }).collectAsList()
  }

  def execute(sparkSession: SparkSession): Dataset[Row] = {
    import sparkSession.implicits._
    var tweets = Commons.readTweetsFromFile(sparkSession)
    tweets = tweets.filter(tweets("lang") === "en")
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokenized_text")
    tweets=tokenizer.transform(tweets)
    val remover=new StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english")).setStopWords(Array("!","!!",",",".",":",";","?","??","=")).setInputCol("tokenized_text").setOutputCol("filter")
    tweets=remover.transform(tweets)
    tweets = tweets.withColumn("filter", concat_ws(",", $"filter"))
    tweets.select("id","filter").limit(200).map(row =>{
      val x=SentimentAnalyzer.extractSentiments(row.getString(1))
      val prova=x.mkString(" ")
      (row.getString(0),prova.split(",").last.dropRight(1))
    }).toDF("id","value")

    //    val new_tweets=Commons.readTweetsFromFile(sparkSession)
    //   new_tweets.join(df_sentiment,new_tweets("id")===df_sentiment("id")).show(200)
  }

  def apiCall(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkContext
    val sparkContext = getSparkContext()
    sparkContext.setLogLevel("INFO")

    // Create SparkSession
    val sparkSession = SparkSession
      .builder()
      .getOrCreate()

    import sparkSession.implicits._
    execute(sparkSession).groupBy("value").count().map(row => {
      val x = row.getValuesMap(row.schema.fieldNames)
      JSONObject(x).toString()
    }).collectAsList()

  }

}