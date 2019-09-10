package com.gilcu2.preprocessing

import com.gilcu2.exploration.Data
import org.apache.spark.sql.DataFrame

object Preprocessing {

  val accidentIdField = "Accident Index"
  val vehicleIdField = "VehicleReferenceNumber"
  val yearField = "Year"
  val casualtyNumberField = "CasualtyNumber"

  def joinVehiclesWithCasualties(vehicles: DataFrame, casualties: DataFrame): DataFrame = {

    val joinFields = Array(accidentIdField, vehicleIdField, yearField)
    vehicles.join(casualties, joinFields, "left")
  }

  def joinAccidentWithVehiclesCasualties(accidents: DataFrame, vehiclesCasualties: DataFrame): DataFrame = {

    val joinFields = Array(accidentIdField, yearField)
    accidents.join(vehiclesCasualties, joinFields, "inner")
  }

}
