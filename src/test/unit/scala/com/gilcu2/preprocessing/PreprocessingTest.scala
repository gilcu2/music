package com.gilcu2.preprocessing

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._
import com.gilcu2.interfaces.Spark._
import Preprocessing._

class PreprocessingTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "Preprocessing"

  import PreprocessingTestData._

  it should "join the the rows of vehicles and casualties if they refer to the same accident and vehicle" in {

    Given("the dataframes of vehicles and casualties")
    val vehicles = loadCSVFromLineSeq(vehicleLines).cache()
    val casualties = loadCSVFromLineSeq(casualtyLines).cache()

    When("joined by accident and vehicles")
    val vehiclesWithCasualties = Preprocessing.joinVehiclesCasualties(vehicles, casualties).cache()

    Then("the number of columns must be from both without repetitions")
    vehiclesWithCasualties.columns.length shouldBe vehicles.columns.length + casualties.columns.length - 3

    And("the number of rows must be at least the number of rows in casualties")
    vehiclesWithCasualties.count() should be >= casualties.count()

    And("when the  vehicle doesn't have casualties related the columns must be null")
    vehiclesWithCasualties.filter(vehiclesWithCasualties(casualtyNumberField).isNull).count shouldBe 3
  }

}


object PreprocessingTestData {

  val vehicleLines =
    """
      |"Accident Index","Year","VehicleReferenceNumber","VehicleType","ArtTowing","Manoeuvre","VehicleLocationOffRoad","JunctionLocation","Skidding","HitObjectOnCWay","VehicleLeaveCWay","HitObjectOffCWay","FirstPointImpact","JourneyPurpose","ForeignReg","SexOfDriver","AgeBandOfDriver"
      |"100177412005",2005,1,9,0,18,0,2,0,7,0,0,1,5,9,1,6
      |"100177482005",2005,1,9,0,4,0,0,0,0,0,0,2,5,9,1,7
      |"100177482005",2005,2,9,0,4,0,0,0,0,0,0,2,5,9,3,0
      |"100179802005",2005,1,9,0,18,0,1,1,0,0,0,3,5,9,1,4
      |"100179802005",2005,2,9,0,18,3,1,0,0,0,0,4,5,9,1,8
      |"100179852005",2005,1,9,0,18,0,8,1,0,1,7,1,5,9,1,6
      |"100179852005",2005,2,9,0,9,0,8,1,0,1,0,4,5,9,2,10
      |"100179862005",2005,1,9,0,18,0,8,0,0,0,0,1,5,9,1,7
      |"100179862005",2005,2,9,0,4,0,8,0,0,0,0,2,5,9,1,7
      |
    """.cleanLines

  val casualtyLines =
    """
      |"Accident Index","Year","VehicleReferenceNumber","CasualtyNumber","CasualtyClass","Sex","AgeBandOfCasualty","CasualtySeverity","PedLocation","PedMovement","CarPassenger","BusPassenger","PedInjWork","CasTypeCode"
      |"100177412005",2005,1,1,2,2,4,3,0,0,2,0,2,9
      |"100177482005",2005,2,1,2,1,7,3,0,0,1,0,2,9
      |"100177482005",2005,2,2,2,2,7,3,0,0,1,0,2,9
      |"100179802005",2005,2,1,1,1,8,3,0,0,0,0,2,9
      |"100179802005",2005,2,2,2,2,4,3,0,0,2,0,2,9
      |"100179802005",2005,2,3,2,1,4,3,0,0,2,0,2,9
      |"100179852005",2005,1,1,1,1,6,3,0,0,0,0,2,9
      |"100179852005",2005,2,2,1,2,10,2,0,0,0,0,2,9
      |"100179862005",2005,2,1,2,1,2,3,0,0,2,0,2,9
      |
    """.cleanLines


}
