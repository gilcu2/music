package com.gilcu2.exploration

import com.gilcu2.interfaces.Spark.loadCSVFromLineSeq
import com.gilcu2.preprocessing.PreprocessingTestData.accidentLines
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper

class ClusteringTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Clustering"

  it should "create group of accident location connected" in {

    Given("the dataframe join of accident")
    val accidents = loadCSVFromLineSeq(accidentLines)

    And("the expected result")

    When("compute the hot spots")
    val hotSpots = Clustering.findHotSpot(accidents, severity = 1)

    Then("it should be the expected")
  }

}
