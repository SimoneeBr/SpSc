package org.spsc.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, desc,concat_ws}
import org.spsc.job.sentiment.SentimentAnalyzer
import org.spsc.utils.{Commons, SparkHelper}

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

    execute(sparkSession)
  }

  def execute(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    var tweets = Commons.readTweetsFromFile(sparkSession)
    tweets = tweets.filter(tweets("lang") === "en")
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokenized_text")
    tweets=tokenizer.transform(tweets)
    val remover=new StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english")).setStopWords(Array("!","!!",",",".",":",";","?","??","=")).setInputCol("tokenized_text").setOutputCol("filter")
    tweets=remover.transform(tweets)
    tweets = tweets.withColumn("filter", concat_ws(",", $"filter"))
    //tweets.select("filter").limit(10).map(row => SentimentAnalyzer.extractSentiments(row.getString(0))).foreach(x=>println(x.mkString(" ")))
    tweets.select("filter").limit(10).map(row => SentimentAnalyzer.extractSentiments(row.getString(0))).map(x=>{
      val prova=x.mkString(" ")
      prova.split(",").last.dropRight(1)
    })



  }

}