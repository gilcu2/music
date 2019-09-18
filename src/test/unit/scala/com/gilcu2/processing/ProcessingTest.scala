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

  it should "compute the most reproduces songs from the longest session" in {

    Given("the tracks files")

  }

}

object DataSample {

  val trackLines=
    """
      |user_000001     2009-05-04T23:08:57Z    f1b1cf71-bd35-4e99-8624-24a6e15f133a    Deep Dish               Fuck Me Im Famous (Pacha Ibiza)-09-28-2007
      |user_000001     2009-05-04T13:54:10Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Composition 0919 (Live_2009_4_15)
      |user_000001     2009-05-04T13:52:04Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Mc2 (Live_2009_4_15)
      |user_000001     2009-05-04T13:42:52Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Hibari (Live_2009_4_15)
      |user_000001     2009-05-04T13:42:11Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Mc1 (Live_2009_4_15)
      |user_000001     2009-05-04T13:38:31Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                To Stanford (Live_2009_4_15)
      |user_000001     2009-05-04T13:33:28Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Improvisation (Live_2009_4_15)
      |user_000001     2009-05-04T13:23:45Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Glacier (Live_2009_4_15)
      |user_000001     2009-05-04T13:19:22Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Parolibre (Live_2009_4_15)
      |user_000001     2009-05-04T13:13:38Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Bibo No Aozora (Live_2009_4_15)
      |user_000001     2009-05-04T13:06:09Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一        f7c1f8f8-b935-45ed-8fc8-7def69d92a10    The Last Emperor (Theme)
      |user_000001     2009-05-04T13:00:48Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Happyend (Live_2009_4_15)
      |user_000001     2009-05-04T12:55:34Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一        475d4e50-cebb-4cd0-8cd4-c3df97987962    Tibetan Dance (Version)
      |user_000001     2009-05-04T12:51:26Z    a7f7df4a-77d8-4f12-8acd-5c60c93f4de8    坂本龍一                Behind The Mask (Live_2009_4_15)
      |user_000001     2009-05-03T15:48:25Z    ba2f4f3b-0293-4bc8-bb94-2f73b5207343    Underworld      dc394163-2b78-4b56-94e4-658597a29ef8    Boy, Boy, Boy (Switch Remix)
      |user_000001     2009-05-03T15:37:56Z    ba2f4f3b-0293-4bc8-bb94-2f73b5207343    Underworld      340d9a0b-9a43-4098-b116-9f79811bd508    Crocodile (Innervisions Orchestra Mix)
      |""".cleanLines


}
