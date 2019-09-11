package com.gilcu2.exploration

import com.gilcu2.interfaces.Spark.loadCSVFromLineSeq
import com.gilcu2.preprocessing.PreprocessingTestData.vehicleLines
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper

class StatisticTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Statistic"

  it should "compute the frequency of the values in the column" in {

    Given("the dataframes of vehicles")
    val vehicles = loadCSVFromLineSeq(vehicleLines)

    And("the expected results")
    val expected = Set(
      (0, 1),
      (4, 1),
      (6, 2),
      (7, 3),
      (8, 1),
      (10, 1)
    )

    When("compute the frequency of the age band of driver")
    val ageRangeOfDriver = "AgeBandOfDriver"
    val freq = Statistic.computeFrequency(vehicles, ageRangeOfDriver)

    Then("results must be the expected")
    freq.collect().map(row => (row(0), row(1))).toSet shouldBe expected

  }

}
