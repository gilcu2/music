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

    When("joined by accident and vehicles ids and year")
    val vehiclesWithCasualties = Preprocessing.joinVehiclesWithCasualties(vehicles, casualties).cache()

    Then("the number of columns must be from both without repetitions")
    vehiclesWithCasualties.columns.length shouldBe vehicles.columns.length + casualties.columns.length - 3

    And("the number of rows must be at least the number of rows in casualties")
    vehiclesWithCasualties.count() should be >= casualties.count()

    And("when the  vehicle doesn't have casualties related the columns must be null")
    vehiclesWithCasualties.filter(vehiclesWithCasualties(casualtyNumberField).isNull).count shouldBe 3
  }

  it should "join the the rows of accidents with vehicles and casualties if they refer to the same accident" in {

    Given("the dataframes of accident and vehiclesWithCasualties")
    val accidents = loadCSVFromLineSeq(accidentLines)
    val vehicles = loadCSVFromLineSeq(vehicleLines).cache()
    val casualties = loadCSVFromLineSeq(casualtyLines).cache()
    val vehiclesWithCasualties = Preprocessing.joinVehiclesWithCasualties(vehicles, casualties).cache()

    When("joined by accident id and year")
    val fullAccidents = Preprocessing.joinAccidentWithVehiclesCasualties(accidents, vehiclesWithCasualties)

    Then("the number of columns must be from both without repetitions")
    fullAccidents.columns.length shouldBe accidents.columns.length + vehiclesWithCasualties.columns.length - 2

    And("the number of rows must be  the number of rows in vehiclesWithCasualties")
    fullAccidents.count() shouldBe vehiclesWithCasualties.count()

  }

}


object PreprocessingTestData {

  val accidentLines =
    """
      |Accident_Index,Location_Easting_OSGR,Location_Northing_OSGR,Longitude,Latitude,Police_Force,Accident_Seveity,Number_of_Vehicles,Number_of_Casualties,Date,Day_of_Week,Time,Local_Authority_(District),Local_Authority_(Highway),1st_Road_Class,1st_Road_Number,Road_Type,Speed_limit,Junction_Detail,Junction_Control,2nd_Road_Class,2nd_Road_Number,Pedestrian_Crossing-Human_Control,Pedestrian_Crossing-Physical_Facilities,Light_Conditions,Weather_Conditions,Road_Surface_Conditions,Special_Conditions_at_Site,Carriageway_Hazards,Urban_or_Rural_Area,Did_Police_Officer_Attend_Scene_of_Accident,LSOA_of_Accident_Location
      |197901A11AD14,,,,,1,3,2,1,18/01/1979,5,08:00,11,9999,3,4,1,30,1,4,-1,-1,-1,-1,1,8,1,-1,0,-1,-1,
      |197901A1BAW34,198460,894000,NULL,NULL,1,3,1,1,01/01/1979,2,01:00,23,9999,6,0,9,30,3,4,-1,-1,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1BFD77,406380,307000,NULL,NULL,1,3,2,3,01/01/1979,2,01:25,17,9999,3,112,9,30,6,4,-1,-1,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1BGC20,281680,440000,NULL,NULL,1,3,2,2,01/01/1979,2,01:30,2,9999,3,502,-1,30,3,2,-1,-1,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1BGF95,153960,795000,NULL,NULL,1,2,2,1,01/01/1979,2,01:30,510,9999,3,309,6,30,0,-1,-1,0,-1,-1,4,3,3,-1,0,-1,-1,
      |197901A1CBC96,300370,146000,NULL,NULL,1,3,1,1,01/01/1979,2,02:05,9,9999,3,23,6,30,3,2,-1,-1,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1DAK71,143370,951000,NULL,NULL,1,3,2,2,01/01/1979,2,03:00,27,9999,4,454,9,30,0,-1,-1,0,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1DAP95,471960,845000,NULL,NULL,1,3,2,1,01/01/1979,2,03:00,19,9999,5,0,9,30,3,4,-1,-1,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1EAC32,323880,632000,NULL,NULL,1,2,1,1,01/01/1979,2,04:00,3,9999,3,105,6,30,3,4,-1,-1,-1,-1,4,3,3,-1,0,-1,-1,
      |197901A1FBK75,136380,245000,NULL,NULL,1,3,2,1,01/01/1979,2,05:05,27,9999,4,455,9,30,0,-1,-1,0,-1,-1,4,8,3,-1,0,-1,-1,
    """.cleanLines

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
    """.cleanLines

}
