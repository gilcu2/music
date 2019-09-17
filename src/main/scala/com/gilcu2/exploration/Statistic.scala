package com.gilcu2.exploration

import com.gilcu2.preprocessing.Preprocessing
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

  def computeAbsoluteFrequency(df: DataFrame, targetField: String, targetValue: Int, primaryField: String): Seq[(Int, Long)] = {
    val targetDf = df.filter(df(targetField) === targetValue)
    computeFrequency(targetDf, primaryField)
      .collect
      .map(row => (row.getInt(0), row.getLong(1)))
      .toSeq
  }

  def computeRelativeFrequency(df: DataFrame, targetField: String, targetValue: Int, primaryField: String): Seq[(Int, Double)] = {

    val totalFrequency = computeFrequency(df, primaryField)
      .collect.map(row => (row.getInt(0), row.getLong(1)))
      .toMap

    val targetFrequency = computeAbsoluteFrequency(df, targetField, targetValue, primaryField)

    targetFrequency
      .map { case (value, frequency) => (value, frequency.toDouble / totalFrequency(value)) }
      .sortBy(_._2)

  }

  def showSeverityAgainstAccidentFields(accidents: DataFrame, severity: Int, fields: Seq[String]): Unit = {

    fields.foreach(field => {
      val freq = computeAbsoluteFrequency(accidents, accidentSeveriteField, severity, field)
      println(s"Field: $field")
      freq.foreach(v => println(s"${v._1} ${v._2}"))
    })

  }

  def showSeverityAgainstVehicleFields(accidents: DataFrame, vehicles: DataFrame, severity: Int, fields: Seq[String]): Unit = {
    val accidentVehicles = Preprocessing.joinAccidentWithVehicles(accidents, vehicles)

    fields.foreach(field => {
      val freq = computeAbsoluteFrequency(accidentVehicles, accidentSeveriteField, severity, field)
      println(s"Field: $field")
      freq.foreach(v => println(s"${v._1} ${v._2}"))
    })

  }

  def showRelativeSeverityAgainstAccidentFields(accidents: DataFrame, severity: Int, fields: Seq[String]): Unit = {

    fields.foreach(field => {
      val freq = computeRelativeFrequency(accidents, accidentSeveriteField, severity, field)
      println(s"Field: $field")
      freq.foreach(v => println(s"${v._1} ${v._2}"))
    })

  }

  def showRelativeSeverityAgainstVehicleFields(accidents: DataFrame, vehicles: DataFrame, severity: Int, fields: Seq[String]): Unit = {
    val accidentVehicles = Preprocessing.joinAccidentWithVehicles(accidents, vehicles)

    fields.foreach(field => {
      val freq = computeRelativeFrequency(accidentVehicles, accidentSeveriteField, severity, field)
      println(s"Field: $field")
      freq.foreach(v => println(s"${v._1} ${v._2}"))
    })

  }

}
