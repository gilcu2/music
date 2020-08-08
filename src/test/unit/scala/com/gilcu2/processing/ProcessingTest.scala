package com.gilcu2.processing

import com.gilcu2.interfaces.Spark.loadCSVFromLineSeq
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import DataSample._
import testUtil.UtilTest._

class ProcessingTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Processing"

  it should "prepare the dataframe" in {

    Given("the tracks lines")
    val originalTracks=loadCSVFromLineSeq(trackLines,delimiter = "\t",header = false).cache()

    When("prepared")
    val tracks=Processing.prepareData(originalTracks)
    tracks.printSchema()

    Then("the columns names must be the expected")
    tracks.columns shouldBe Processing.fields

  }

  it should "find the two user sessions from the same user" in {

    Given("the tracks lines with 2 session of the same user")
    val trackLines =
      """
        |user_000001	2009-05-04T13:08:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	artist1	idsong1	song1
        |user_000001	2009-05-04T13:14:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist2	idsong2	song2
        |user_000001	2009-05-04T13:52:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist3	idsong3	song3
    """.cleanLines
    val originalTracks=loadCSVFromLineSeq(trackLines,delimiter = "\t",header = false).cache()
    val tracks=Processing.prepareData(originalTracks)

    When("compute the sessions")
    val sessions = Processing.computeSessions(tracks).collect()

    Then("then sessions must be the expected")
    sessions.size shouldBe 2
    sessions(0).getList(1).size() shouldBe 2
    sessions(1).getList(1).size() shouldBe 1
  }

  it should "find the sessions from two users" in {

    Given("the tracks lines with 2 session of the same user")
    val trackLines =
      """
        |user_000001	2009-05-04T13:08:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	artist1	idsong1	song1
        |user_000002	2009-05-04T13:14:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist2	idsong2	song2
        |user_000001	2009-05-04T13:27:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist3	idsong3	song3
        |user_000002	2009-05-04T13:33:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	artist1	idsong1	song1
        |user_000002	2009-05-04T13:40:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist2	idsong2	song2
        |user_000001	2009-05-04T13:45:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist3	idsong3	song3
    """.cleanLines
    val originalTracks = loadCSVFromLineSeq(trackLines, delimiter = "\t", header = false).cache()
    val tracks = Processing.prepareData(originalTracks)

    When("compute the sessions")
    val sessions = Processing.computeSessions(tracks)

    Then("then sessions must be the expected")
    sessions.count shouldBe 2
    sessions.collect.map(_.getString(0)).toSet shouldBe Set("user_000001", "user_000002")
  }

  it should "return the longest sessions" in {

    Given("the tracks lines with 4 session of two users")
    val trackLines =
      """
        |user_000001	2009-05-04T13:08:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	artist1	idsong1	song1
        |user_000002	2009-05-04T13:14:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist2	idsong2	song2
        |user_000001	2009-05-04T13:27:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist3	idsong3	song3
        |user_000002	2009-05-04T13:33:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	artist1	idsong1	song1
        |user_000002	2009-05-04T13:40:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist2	idsong2	song2
        |user_000001	2009-05-04T13:45:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist3	idsong3	song3
        |user_000002	2009-05-04T14:33:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	artist1	idsong1	song1
        |user_000002	2009-05-04T14:40:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist2	idsong2	song2
        |user_000001	2009-05-04T14:45:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist3	idsong3	song3
    """.cleanLines
    val originalTracks = loadCSVFromLineSeq(trackLines, delimiter = "\t", header = false).cache()
    val tracks = Processing.prepareData(originalTracks)
    val sessions = Processing.computeSessions(tracks)

    When("compute the longest sessions")
    val longestSessions = Processing.computeLongestSessions(sessions, 3)

    Then("then sessions must be the expected")
    longestSessions.count shouldBe 3
    longestSessions.collect.map(_.getString(0)).sorted shouldBe Seq("user_000001", "user_000002", "user_000002")
  }


  it should "compute the 2 most reproduces songs from the longest session" in {

    Given("the tracks lines with 2 session of the same user")
    val trackLines =
      """
        |user_000001	2009-05-04T13:08:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	artist1	idsong1	song1
        |user_000002	2009-05-04T13:14:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist2	idsong2	song2
        |user_000001	2009-05-04T13:27:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist3	idsong3	song3
        |user_000002	2009-05-04T13:33:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	artist1	idsong1	song1
        |user_000002	2009-05-04T13:40:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist2	idsong2	song2
        |user_000001	2009-05-04T13:45:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist3	idsong3	song3
        |user_000002	2009-05-04T14:33:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	artist1	idsong1	song1
        |user_000002	2009-05-04T14:40:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist2	idsong2	song2
        |user_000001	2009-05-04T14:45:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	artist3	idsong3	song3
    """.cleanLines
    val originalTracks = loadCSVFromLineSeq(trackLines, delimiter = "\t", header = false).cache()
    val tracks = Processing.prepareData(originalTracks)
    val sessions = Processing.computeSessions(tracks)
    val longestSessions = Processing.computeLongestSessions(sessions, 3)

    When("compute the 2 top sons")
    val topSongs = Processing.computeTopSongs(longestSessions, 2)

    topSongs.printSchema()
    topSongs.show(20, truncate = 120, vertical = true)

    Then("then top song must be the expected")
    topSongs.collect.map(r => (r.getString(0), r.getString(1))).toSet shouldBe Set(("artist1", "song1"), ("artist2", "song2"))
  }

  it should ("compute the number of reproductions per artist") in {
    Given("the tracks lines with 3 artists")
    val trackLines =
      """
        |user_000001	2009-05-04T13:08:57Z	id1	artist1	idsong1	song1
        |user_000002	2009-05-04T13:14:10Z	id2	artist2	idsong2	song2
        |user_000001	2009-05-04T13:27:04Z	id3	artist3	idsong3	song3
        |user_000002	2009-05-04T13:33:57Z	id1	artist1	idsong1	song1
        |user_000002	2009-05-04T13:40:10Z	id2	artist2	idsong2	song2
        |user_000001	2009-05-04T13:45:04Z	id3	artist3	idsong3	song3
        |user_000002	2009-05-04T14:33:57Z	id1	artist1	idsong1	song1
        |user_000002	2009-05-04T14:40:10Z	id2	artist2	idsong2	song2
        |user_000001	2009-05-04T14:45:04Z	id3	artist3	idsong3	song3
    """.cleanLines
    val originalTracks = loadCSVFromLineSeq(trackLines, delimiter = "\t", header = false).cache()
    val tracks = Processing.prepareData(originalTracks)

    When("compute the reproductions per artist")
    val reproductions = Processing.computeReproductionPerArtist(tracks)

    Then("then sessions must be the expected")
    reproductions.count shouldBe 3
    reproductions.collect.map(r => (r.getString(0), r.getLong(1))).sorted.toSeq shouldBe
      Seq(("id1", 3), ("id2", 3), ("id3", 3))


  }

}

object DataSample {

  val trackLines=
    """
      |user_000001	2009-05-04T23:08:57Z	f1b1cf71-bd35-4e99-8624-24a6e15f133a	Deep Dish 	  	Fuck Me Im Famous (Pacha Ibiza)-09-28-2007
      |user_000001	2009-05-04T13:54:10Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Composition 0919 (Live_2009_4_15)
      |user_000001	2009-05-04T13:52:04Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Mc2 (Live_2009_4_15)
      |user_000001	2009-05-04T13:42:52Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Hibari (Live_2009_4_15)
      |user_000001	2009-05-04T13:42:11Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Mc1 (Live_2009_4_15)
      |user_000001	2009-05-04T13:38:31Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		To Stanford (Live_2009_4_15)
      |user_000001	2009-05-04T13:33:28Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Improvisation (Live_2009_4_15)
      |user_000001	2009-05-04T13:23:45Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Glacier (Live_2009_4_15)
      |user_000001	2009-05-04T13:19:22Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Parolibre (Live_2009_4_15)
      |user_000001	2009-05-04T13:13:38Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Bibo No Aozora (Live_2009_4_15)
      |user_000002	2009-05-04T13:06:09Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		The Last Emperor (Theme)
      |user_000002	2009-05-04T13:00:48Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Happyend (Live_2009_4_15)
      |user_000002	2009-05-04T12:55:34Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Tibetan Dance (Version)
      |user_000001	2009-05-04T12:51:26Z	a7f7df4a-77d8-4f12-8acd-5c60c93f4de8	坂本龍一      		Behind The Mask (Live_2009_4_15)
      |user_000002	2009-05-03T15:48:25Z	ba2f4f3b-0293-4bc8-bb94-2f73b5207343	Underworld	  	Boy, Boy, Boy (Switch Remix)
      |user_000001	2009-05-03T15:37:56Z	ba2f4f3b-0293-4bc8-bb94-2f73b5207343	Underworld	  	Crocodile (Innervisions Orchestra Mix)
      |""".cleanLines


}
