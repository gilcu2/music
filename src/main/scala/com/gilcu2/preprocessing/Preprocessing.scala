package com.gilcu2.preprocessing

import com.gilcu2.exploration.Data
import org.apache.spark.sql.DataFrame

object Preprocessing {

  val accidentId = "Accident Index"
  val vehicleId = "Vehicle Reference"

  def joinVehiclesCausalties(data: Data): DataFrame = {
    val vehicles = data.vehicles
    val casualties = data.casualties
    vehicles.join(casualties, vehicles(accidentId) === casualties(accidentId) &&
      vehicles(vehicleId) === casualties(vehicleId), "left")
  }

}
