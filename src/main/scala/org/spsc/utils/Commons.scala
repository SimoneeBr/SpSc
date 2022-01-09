package org.spsc.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.spsc.model.{PlaceObject, Tweet, User}

object Commons {

  def globalQueryJoined(sparkSession: SparkSession): Dataset[Row] = {

    val tweetDF = readTweetsFromFile(sparkSession)
    val placeDF = readPlacesFromFile(sparkSession)

    // Filtered Tweet DataFrame
    val filteredTweetDF = tweetDF.filter(tweetDF("geo").isNotNull)

    // Geo DataFrame
    val geo = filteredTweetDF.select(col("id"), col("geo.*"))
    val geoFlattenDF = geo.toDF("tweet_id", "geo_place_id", "geo_coordinates")

    // Tweet + Geo
    val tweetAndGeo = filteredTweetDF.join(geoFlattenDF, filteredTweetDF("id") === geoFlattenDF("tweet_id"))

    // Tweet + Geo + Place
    tweetAndGeo.join(placeDF, tweetAndGeo("geo_place_id") === placeDF("id"))

  }


  def usersJoined(sparkSession: SparkSession): Dataset[Row] = {
    val tweetDF = readTweetsFromFile(sparkSession)
    val userDF = readUsersFromFile(sparkSession)
    // Tweet + User
    tweetDF.join(userDF, tweetDF("author_id") === userDF("id"))
  }

  def readTweetsFromFile(sparkSession: SparkSession): Dataset[Row] = {
    val encoderTweetSchema = Encoders.product[Tweet].schema
    sparkSession.read.schema(encoderTweetSchema).json(Constants.tweetFile)
  }

  def readPlacesFromFile(sparkSession: SparkSession): Dataset[Row] = {
    val encoderPlaceSchema = Encoders.product[PlaceObject].schema
    sparkSession.read.schema(encoderPlaceSchema).json(Constants.placeFile)
  }

  def readUsersFromFile(sparkSession: SparkSession): Dataset[Row] = {
    val encoderUserSchema = Encoders.product[User].schema
    sparkSession.read.schema(encoderUserSchema).json(Constants.userFile)
  }

}
