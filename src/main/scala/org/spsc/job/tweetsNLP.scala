package org.spsc.job

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.spsc.utils.{Commons, SparkHelper}

object tweetsNLP extends SparkHelper{

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
    //FIXME NOT WORKING
    val tweets = Commons.readTweetsFromFile(sparkSession)
    val explainDocumentPipeline = PretrainedPipeline("explain_document_ml")
    val annotations_df = explainDocumentPipeline.transform(tweets.select("text"))
    annotations_df.show()
  }
}
