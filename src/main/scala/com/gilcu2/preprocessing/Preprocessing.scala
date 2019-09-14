package com.gilcu2.preprocessing

import com.gilcu2.exploration.Data
import org.apache.spark.sql.DataFrame

object Preprocessing {

  val accidentIdField = "Accident_Index"
  val vehicleIdField = "Vehicle_Reference"
  val casualtyNumberField = "Casualty_Reference"
  val accidentSeveriteField = "Accident_Severity"
  val driverAgeField = "Age_Band_of_Driver"
  val sexDriverField = "Sex_of_Driver"
  val lightConditionField = "Lighting_Conditions"
  val weatherConditionField = "Weather_Conditions"
  val roadConditionField = "Road_Surface_Conditions"

  def joinVehiclesWithCasualties(vehicles: DataFrame, casualties: DataFrame): DataFrame = {

    val joinFields = Array(accidentIdField, vehicleIdField)
    vehicles.join(casualties, joinFields, "left")
  }

  def joinAccidentWithVehiclesCasualties(accidents: DataFrame, vehiclesCasualties: DataFrame): DataFrame = {

    val joinFields = Array(accidentIdField)
    accidents.join(vehiclesCasualties, joinFields, "inner")
  }

  def joinAccidentWithVehicles(accidents: DataFrame, vehicles: DataFrame): DataFrame = {

    val joinFields = Array(accidentIdField)
    accidents.join(vehicles, joinFields, "inner")
  }

  def joinAccidentsWithCasualties(accidents: DataFrame, casualties: DataFrame): DataFrame = {

    val joinFields = Array(accidentIdField)
    accidents.join(casualties, joinFields, "inner")
  }

}
