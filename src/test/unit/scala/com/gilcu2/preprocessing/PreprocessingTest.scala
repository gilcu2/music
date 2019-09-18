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
    vehiclesWithCasualties.columns.length shouldBe vehicles.columns.length + casualties.columns.length - 2

    And("the number of rows must be at least the number of rows in casualties")
    vehiclesWithCasualties.count() should be >= casualties.count()

    And("when the  vehicle doesn't have casualties related the columns must be null")
    vehiclesWithCasualties.filter(vehiclesWithCasualties(casualtyNumberField).isNull).count shouldBe 5
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
    fullAccidents.columns.length shouldBe accidents.columns.length + vehiclesWithCasualties.columns.length - 1

    And("the number of rows must be  the number of rows in vehiclesWithCasualties")
    fullAccidents.count() shouldBe vehiclesWithCasualties.count()

  }

}

object PreprocessingTestData {

  val accidentLines =
    """
      |Accident_Index,Location_Easting_OSGR,Location_Northing_OSGR,Longitude,Latitude,Police_Force,Accident_Severity,Number_of_Vehicles,Number_of_Casualties,Date,Day_of_Week,Time,Local_Authority_(District),Local_Authority_(Highway),1st_Road_Class,1st_Road_Number,Road_Type,Speed_limit,Junction_Detail,Junction_Control,2nd_Road_Class,2nd_Road_Number,Pedestrian_Crossing-Human_Control,Pedestrian_Crossing-Physical_Facilities,Light_Conditions,Weather_Conditions,Road_Surface_Conditions,Special_Conditions_at_Site,Carriageway_Hazards,Urban_or_Rural_Area,Did_Police_Officer_Attend_Scene_of_Accident,LSOA_of_Accident_Location
      |197901A11AD14,,,,,1,1,2,1,18/01/1979,5,08:00,11,9999,3,4,1,30,1,4,-1,-1,-1,-1,1,8,1,-1,0,-1,-1,
      |197901A1BAW34,198460,894000,NULL,NULL,1,1,1,1,01/01/1979,2,01:00,23,9999,6,0,9,30,3,4,-1,-1,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1BFD77,406380,307000,NULL,NULL,1,1,2,3,01/01/1979,2,01:25,17,9999,3,112,9,30,6,4,-1,-1,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1BGC20,281680,440000,NULL,NULL,1,3,2,2,01/01/1979,2,01:30,2,9999,3,502,-1,30,3,2,-1,-1,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1BGF95,153960,795000,NULL,NULL,1,2,2,1,01/01/1979,2,01:30,510,9999,3,309,6,30,0,-1,-1,0,-1,-1,4,3,3,-1,0,-1,-1,
      |197901A1DAK71,143370,951000,NULL,NULL,1,3,2,2,01/01/1979,2,03:00,27,9999,4,454,9,30,0,-1,-1,0,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1DAP95,471960,845000,NULL,NULL,1,3,2,1,01/01/1979,2,03:00,19,9999,5,0,9,30,3,4,-1,-1,-1,-1,4,8,3,-1,0,-1,-1,
      |197901A1EAC32,323880,632000,NULL,NULL,1,1,1,1,01/01/1979,2,04:00,3,9999,3,105,6,30,3,4,-1,-1,-1,-1,4,3,3,-1,0,-1,-1,
      |197901A1FBK75,136380,245000,NULL,NULL,1,1,2,1,01/01/1979,2,05:05,27,9999,4,455,9,30,0,-1,-1,0,-1,-1,4,8,3,-1,0,-1,-1,
    """.cleanLines

  val vehicleLines =
    """
      |Accident_Index,Vehicle_Reference,Vehicle_Type,Towing_and_Articulation,Vehicle_Manoeuvre,Vehicle_Location-Restricted_Lane,Junction_Location,Skidding_and_Overturning,Hit_Object_in_Carriageway,Vehicle_Leaving_Carriageway,Hit_Object_off_Carriageway,1st_Point_of_Impact,Was_Vehicle_Left_Hand_Drive?,Journey_Purpose_of_Driver,Sex_of_Driver,Age_Band_of_Driver,Engine_Capacity_(CC),Propulsion_Code,Age_of_Vehicle,Driver_IMD_Decile,Driver_Home_Area_Type
      |197901A11AD14,1,109,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,7,-1,-1,-1,-1,-1
      |197901A11AD14,2,104,0,13,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,-1,-1,-1,-1,-1,-1
      |197901A1BAW34,1,109,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,-1,-1,-1,-1,-1,-1
      |197901A1BFD77,1,109,0,18,-1,-1,1,-1,-1,-1,-1,-1,-1,1,5,-1,-1,-1,-1,-1
      |197901A1BFD77,2,109,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,7,-1,-1,-1,-1,-1
      |197901A1BGC20,1,109,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,7,-1,-1,-1,-1,-1
      |197901A1BGC20,2,109,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,-1,-1,-1,-1,-1,-1
      |197901A1BGF95,1,109,0,13,-1,-1,-1,-1,-1,-1,-1,-1,-1,2,5,-1,-1,-1,-1,-1
      |197901A1BGF95,2,109,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,7,-1,-1,-1,-1,-1
      |197901A1DAK71,1,109,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,2,6,-1,-1,-1,-1,-1
      |197901A1DAK71,2,109,0,13,-1,-1,1,-1,-1,-1,-1,-1,-1,1,5,-1,-1,-1,-1,-1
      |197901A1DAP95,1,109,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,6,-1,-1,-1,-1,-1
      |197901A1DAP95,2,109,0,7,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,5,-1,-1,-1,-1,-1
      |197901A1EAC32,1,90,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
      |197901A1FBK75,1,109,0,18,-1,-1,-1,-1,-1,-1,-1,-1,-1,1,6,-1,-1,-1,-1,-1
      |197901A1FBK75,2,109,0,18,-1,-1,1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
    """.cleanLines

  val casualtyLines =
    """
      |Accident_Index,Vehicle_Reference,Casualty_Reference,Casualty_Class,Sex_of_Casualty,Age_Band_of_Casualty,Casualty_Severity,Pedestrian_Location,Pedestrian_Movement,Car_Passenger,Bus_or_Coach_Passenger,Pedestrian_Road_Maintenance_Worker,Casualty_Type,Casualty_Home_Area_Type
      |197901A11AD14,2,1,1,1,-1,3,0,0,0,0,0,104,-1
      |197901A1BAW34,1,1,3,2,6,3,10,5,0,0,-1,0,-1
      |197901A1BFD77,1,1,1,1,5,3,0,0,0,0,0,109,-1
      |197901A1BFD77,1,2,2,1,4,3,0,0,1,0,0,109,-1
      |197901A1BFD77,2,3,1,1,6,3,0,0,0,0,0,109,-1
      |197901A1BGC20,1,1,2,1,8,3,0,0,1,0,0,109,-1
      |197901A1BGC20,1,2,2,2,8,3,0,0,2,0,0,109,-1
      |197901A1BGF95,1,1,2,2,9,2,0,0,1,0,0,109,-1
      |197901A1CBC96,1,1,3,1,-1,3,5,9,0,0,-1,0,-1
      |197901A1DAK71,1,1,2,1,7,3,0,0,1,0,0,109,-1
      |197901A1DAK71,2,2,1,1,5,3,0,0,0,0,0,109,-1
      |197901A1DAP95,1,1,2,2,4,3,0,0,1,0,0,109,-1
      |197901A1EAC32,1,1,3,1,5,2,10,5,0,0,-1,0,-1
      |197901A1FBK75,1,1,1,1,5,3,0,0,0,0,0,109,-1

    """.cleanLines

}
