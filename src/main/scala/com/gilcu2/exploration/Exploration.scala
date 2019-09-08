package com.gilcu2.exploration

import org.apache.spark.sql.DataFrame

case class Data(accidents: DataFrame, vehicles: DataFrame, casualties: DataFrame)

object Exploration {

  val accidentLabel = "Accidents"
  val vehicleLabel = "Vehicles"
  val casualtyLabel = "Casualties"

  def showSummaries(data: Data): Unit = {

    def summaryAndShow(df: DataFrame, name: String): Unit = {
      val summary = df.summary()
      println(s"\n$name")
      summary.show()
    }

    summaryAndShow(data.accidents, accidentLabel)
    summaryAndShow(data.vehicles, vehicleLabel)
    summaryAndShow(data.casualties, casualtyLabel)

  }

}
