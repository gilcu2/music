package com.gilcu2.processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import java.sql.Timestamp

object Processing {

  val userIdField = "userId"
  val timeField = "timeStamp"
  val artistIdField = "artistId"
  val artistNameField = "artistName"
  val songIdField = "songId"
  val songNameField = "songName"
  val timeStampsField = "timeStamps"
  val artistNamesField = "artistNames"
  val songNamesField = "songNames"
  val userTracksField = "userTracks"
  val userSessionsField = "userSessions"

  val fields = Array(userIdField, timeField, artistIdField, artistNameField, songIdField, songNameField)

  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  def prepareData(df: DataFrame)(implicit spark: SparkSession): DataFrame =
    df
      .withColumn(userIdField, trim(col("_c0")))
      .withColumn(timeField, trim(col("_c1")))
      .withColumn(artistIdField, trim(col("_c2")))
      .withColumn(artistNameField, trim(col("_c3")))
      .withColumn(songIdField, trim(col("_c4")))
      .withColumn(songNameField, trim(col("_c5")))
      .select(userIdField, timeField, artistIdField, artistNameField, songIdField, songNameField)

  val zipperAndSort = udf[Seq[(Timestamp, String, String)], Seq[Timestamp], Seq[String], Seq[String]](
    (times, artists, songs) => {
      times.zip(artists).zip(songs).map(t => (t._1._1, t._1._2, t._2))
        .sortBy(_._1)
    })

  val computeSessions = udf[Seq[Seq[(Timestamp, String, String)]], Seq[(Timestamp, String, String)]](
    tracks => {
      val sessions = scala.collection.mutable.ListBuffer[Seq[(Timestamp, String, String)]]()
      var session = scala.collection.mutable.ListBuffer(tracks.head)
      var lastMinutes = tracks.head._1.getTime / 60000
      tracks.tail.foreach { case (time, artist, song) => {
        val minutes = time.getTime / 60000
        if (minutes - lastMinutes < 20)
          session.append((time, artist, song))
        else {
          sessions.append(session)
          session = scala.collection.mutable.ListBuffer((time, artist, song))
        }
        lastMinutes = minutes
      }
      }
      sessions
    })


  def computeLongestSessions(tracks: DataFrame, sessions: Int)(implicit spark: SparkSession): DataFrame = {

    tracks.printSchema()

    val userTracks = tracks
      .groupBy(userIdField)
      .agg(
        collect_list(timeField).as(timeStampsField),
        collect_list(artistNameField).as(artistNamesField),
        collect_list(songNameField).as(songNamesField)
      )
      .withColumn(userTracksField, zipperAndSort(col(timeStampsField),
        col(artistNamesField), col(songNamesField)))
      .select(col(userIdField), col(userTracksField))


    userTracks.printSchema()
    userTracks.show(20, truncate = 120, vertical = true)

    val userSessions = userTracks
      .withColumn(userSessionsField, computeSessions(col(userTracksField)))
      .select(col(userIdField), col(userSessionsField))

    userSessions.printSchema()
    userSessions.show(20, truncate = 120, vertical = true)

    userTracks
  }

  def getSessions(timestamps: Column, artistNames: Column, songNames: Column): Column = {

    val len = size(timestamps)
    len
  }

  def computeTopFromLongestSessions(tracks: DataFrame, top: Int, sessions: Int)(implicit spark: SparkSession): DataFrame = {
    spark.emptyDataFrame
  }

}
