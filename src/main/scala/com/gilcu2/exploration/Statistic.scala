package com.gilcu2.exploration

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.gilcu2.preprocessing.Preprocessing._

object Statistic {

  val labelFrequency = "Frequency"


  def computeFrequency(df: DataFrame, column: String): DataFrame =
    df.select(column)
      .withColumn(labelFrequency, count(column).over(Window.partitionBy(column)))
      .distinct()
      .sort(labelFrequency)

  def computeSeverityDriverAge(accidentVehicles: DataFrame, severity: Int): DataFrame = {
    val filtered = accidentVehicles.filter(accidentVehicles(accidentSeveriteField) === severity)
    computeFrequency(filtered, vehicleDriverAgeField)

  }

}
