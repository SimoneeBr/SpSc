package org.spsc.job

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
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
    var tweets = Commons.readTweetsFromFile(sparkSession)
    tweets = tweets.filter(tweets("lang") === "en")
    val pipeline = new PretrainedPipeline("analyze_sentimentdl_use_twitter", "en")
    val result = pipeline.annotate(tweets, "text")
    result.select("text", "sentiment").show(false)
  }

//  def execute(sparkSession: SparkSession): Unit = {
//    val document = DocumentAssembler()
//      .setInputCol("text")
//      .setOutputCol("document")
//
//    val embeddings = BertSentenceEmbeddings
//      .pretrained("labse", "xx")
//      .setInputCols(Array("document"))
//      .setOutputCol("sentence_embeddings")
//
//    val sentimentClassifier = ClassifierDLModel.pretrained("classifierdl_bert_sentiment", "es")
//      .setInputCols(Array("document", "sentence_embeddings"))
//      .setOutputCol("class")
//
//    val fr_sentiment_pipeline = new Pipeline().setStages(Array(document, embeddings, sentimentClassifier))
//
//    val light_pipeline = LightPipeline(fr_sentiment_pipeline.fit(spark.createDataFrame([['']]).toDF("text")))
//
//    val result1 = light_pipeline.annotate("Estoy seguro de que esta vez pasará la entrevista.")
//
//    val result2 = light_pipeline.annotate("Soy una persona que intenta desayunar todas las mañanas sin falta.")
//
//    val result3 = light_pipeline.annotate("No estoy seguro de si mi salario mensual es suficiente para vivir.")
//  }

}