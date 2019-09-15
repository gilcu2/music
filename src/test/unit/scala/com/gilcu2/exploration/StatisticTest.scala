package com.gilcu2.exploration

import com.gilcu2.interfaces.Spark.loadCSVFromLineSeq
import com.gilcu2.preprocessing.Preprocessing
import com.gilcu2.preprocessing.PreprocessingTestData._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import Preprocessing._

class StatisticTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Statistic"

  it should "compute the frequency of the values in the column" in {

    Given("the dataframes of vehicles")
    val vehicles = loadCSVFromLineSeq(vehicleLines)

    And("the expected results")
    val expected = Set(
      (-1, 5),
      (5, 4),
      (6, 3),
      (7, 5)
    )

    When("compute the frequency of the age band of driver")
    val freq = Statistic.computeFrequency(vehicles, driverAgeField)

    Then("results must be the expected")
    freq.collect().map(row => (row(0), row(1))).toSet shouldBe expected

  }

  it should "compute the absolute frequency with respect to a target value" in {

    Given("the dataframe join of accident and vehicles ")
    val accidents = loadCSVFromLineSeq(accidentLines)
    val vehicles = loadCSVFromLineSeq(vehicleLines)
    val accidentVehicles = Preprocessing.joinAccidentWithVehicles(accidents, vehicles)

    And("the expected results")
    val expected = Set(
      (-1, 1),
      (7, 1)
    )

    When("compute the  frequency of the age band of driver when the accident is fatal")
    val fatalSeverity = 1
    val freq = Statistic.computeAbsoluteFrequency(accidentVehicles, accidentSeveriteField, fatalSeverity, driverAgeField)

    Then("results must be the expected")
    freq.toSet shouldBe expected

  }

  it should "compute the relative frequency with respect to a target value" in {

    Given("the dataframe join of accident and vehicles ")
    val accidents = loadCSVFromLineSeq(accidentLines)
    val vehicles = loadCSVFromLineSeq(vehicleLines)
    val accidentVehicles = Preprocessing.joinAccidentWithVehicles(accidents, vehicles)

    And("the expected results")
    val expected = Set(
      (-1, 1.0 / 5),
      (7, 1.0 / 5)
    )

    When("compute the  frequency of the age band of driver when the accident is fatal")
    val fatalSeverity = 1
    val freq = Statistic.computeRelativeFrequency(accidentVehicles, accidentSeveriteField, fatalSeverity, driverAgeField)

    Then("results must be the expected")
    freq.toSet shouldBe expected

  }


}
