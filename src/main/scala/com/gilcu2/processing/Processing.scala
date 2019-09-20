package com.gilcu2.processing

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Processing {

  val userIdField = "userId"
  val timeField = "timeStamp"
  val artistIdField = "artistId"
  val artistNameField = "artistName"
  val songIdField = "songId"
  val songNameField = "songName"
  val timeStamps="timeStamps"
  val artistNames="artistNames"
  val songNames="songNames"

  val fields = Array(userIdField, timeField, artistIdField, artistNameField, songIdField, songNameField)

  def prepareData(df: DataFrame)(implicit spark: SparkSession): DataFrame =
    df
      .withColumnRenamed("_c0", userIdField)
      .withColumnRenamed("_c1", timeField)
      .withColumnRenamed("_c2", artistIdField)
      .withColumnRenamed("_c3", artistNameField)
      .withColumnRenamed("_c4", songIdField)
      .withColumnRenamed("_c5", songNameField)

  def computeLongestSessions(tracks: DataFrame, sessions: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val zipper=udf[Seq[(Timestamp,String,String)],Seq[Timestamp],Seq[String],Seq[String]]((times, artists, songs)=>{
      times.zip(artists).zip(songs).map(t=>(t._1._1,t._1._2,t._2))
    })

    tracks.printSchema()

    val userTracks = tracks
      .groupBy(userIdField)
      .agg(
        collect_list(timeField).as(timeStamps),
        collect_list(artistNameField).as(artistNames),
        collect_list(songNameField).as(songNames)
      )
      .withColumn("userTracks", zipper(col(timeStamps),col(artistNames), col(songNames)))
//          .agg(sort_array(collect_list(concat(col(timeField),col(artistNameField),col(songName)))).as("user_tracks"))
    userTracks.printSchema()
    userTracks.show(20, truncate = 80, vertical = true)

//    userTracks.select(col(userIdField), )
    userTracks
  }

  def getSessions(timestamps: Column,artistNames:Column,songNames:Column): Column = {

    val len = size(timestamps)
    len
  }

  def computeTopFromLongestSessions(tracks: DataFrame, top: Int, sessions: Int)(implicit spark: SparkSession): DataFrame = {
    spark.emptyDataFrame
  }

}
