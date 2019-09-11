package com.gilcu2.exploration

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Statistic {

  val labelFrequency = "Frequency"

  def computeFrequency(df: DataFrame, column: String): DataFrame =
    df.select(column)
      .withColumn(labelFrequency, count(column).over(Window.partitionBy(column)))
      .distinct()
}
