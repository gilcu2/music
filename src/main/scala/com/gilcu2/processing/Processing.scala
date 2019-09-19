package com.gilcu2.processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Processing {

  val userIdField = "userId"
  val timeField = "timeStamp"
  val artistIdField = "artistId"
  val artistNameField = "artistName"
  val songIdField = "songId"
  val songName = "songName"

  val fields = Array(userIdField, timeField, artistIdField, artistNameField, songIdField, songName)

  def prepareData(df: DataFrame)(implicit spark: SparkSession): DataFrame =
    df
      .withColumnRenamed("_c0", userIdField)
      .withColumnRenamed("_c1", timeField)
      .withColumnRenamed("_c2", artistIdField)
      .withColumnRenamed("_c3", artistNameField)
      .withColumnRenamed("_c4", songIdField)
      .withColumnRenamed("_c5", songName)

  def computeLongestSessions(tracks: DataFrame, sessions: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val userTracks = tracks
      .groupBy(userIdField)
      .agg(sort_array(collect_list(concat(col(timeField),col(artistNameField),col(songName)))).as("user_tracks"))
    userTracks.printSchema()
    userTracks.show(20,truncate = 80,vertical = true)

    userTracks.select(col(userIdField), getSessions($"user_tracks"))
  }

  def getSessions(userTracks: Column): Column = {

    val len = size(userTracks)
    len
  }

  def computeTopFromLongestSessions(tracks: DataFrame, top: Int, sessions: Int)(implicit spark: SparkSession): DataFrame = {
    spark.emptyDataFrame
  }

}
