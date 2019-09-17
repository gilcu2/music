package com.gilcu2

import com.gilcu2.exploration.{Clustering, Data, Exploration, Statistic}
import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, MainTrait, Spark}
import com.gilcu2.preprocessing.Preprocessing
import com.gilcu2.preprocessing.Preprocessing._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf

object TrafficMain extends MainTrait {

  def explore(df: DataFrame): Unit = {

    val summary = df.summary()
    summary.show()

  }

  override def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    val configExploration = configValues.asInstanceOf[ConfigExploration]
    val argumentsExploration = lineArguments.asInstanceOf[ArgumentsExploration]

    val accidents = Spark.loadCSVFromFile(configExploration.accidentPath)
    val vehicles = Spark.loadCSVFromFile(configExploration.vehiclePath)
    val casualties = Spark.loadCSVFromFile(configExploration.casualtyPath)

    val data = Data(accidents, vehicles, casualties)

    val accidentVehicles = Preprocessing.joinAccidentWithVehicles(data.accidents, data.vehicles)

    if (argumentsExploration.doDommain)
      Exploration.showSummaries(data)

    if (argumentsExploration.doFrequency) {
      val fieldsAccidents = Seq(dayOfWeek, lightConditionField, weatherConditionField, roadConditionField)
      Statistic.showSeverityAgainstAccidentFields(data.accidents, severity = 1, fieldsAccidents)

      val fieldsVehicles = Seq(driverSexField, driverAgeField, ageVehicle, vehicleTypeField)
      Statistic.showSeverityAgainstVehicleFields(data.accidents, data.vehicles, severity = 1, fieldsVehicles)
    }

    if (argumentsExploration.doRelativeFrequency) {
      val fieldsAccidents = Seq(dayOfWeek, lightConditionField, weatherConditionField, roadConditionField)
      Statistic.showRelativeSeverityAgainstAccidentFields(data.accidents, severity = 1, fieldsAccidents)

      val fieldsVehicles = Seq(driverSexField, driverAgeField, ageVehicle, vehicleTypeField)
      Statistic.showRelativeSeverityAgainstVehicleFields(data.accidents, data.vehicles, severity = 1, fieldsVehicles)
    }

    if (argumentsExploration.doHotSpot) {
      import spark.implicits._
      val clustering = Clustering.findHotSpots(data.accidents, severity = 1, minimunAccidents = 2)
      val notUnitary = clustering.filter(_.coordinates.size > 1)
      //      clustering.toDF().write.json("hotspots.json")
      notUnitary.toDF().write.json("hotspots1.json")
    }

  }

  override def getConfigValues(conf: Config): ConfigExploration = {
    val dataPath = conf.getString("DataDir")
    val accidentsFilename = conf.getString("Accidents")
    val vehiclesFilename = conf.getString("Vehicles")
    val casualtiesFilename = conf.getString("Casualties")

    ConfigExploration(
      dataPath + "/" + accidentsFilename,
      dataPath + "/" + vehiclesFilename,
      dataPath + "/" + casualtiesFilename
    )
  }

  override def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): ArgumentsExploration = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val doDomain = parsedArgs.domain()
    val doFrequency = parsedArgs.frequency()
    val doRelativeFrequency = parsedArgs.relativeFrequency()
    val doHotSpot = parsedArgs.hotSpots()

    ArgumentsExploration(doDomain, doFrequency, doRelativeFrequency, doHotSpot)
  }

  case class ConfigExploration(accidentPath: String, vehiclePath: String, casualtyPath: String) extends ConfigValuesTrait

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val domain = opt[Boolean](short = 'd')
    val frequency = opt[Boolean](short = 'f')
    val relativeFrequency = opt[Boolean](short = 'r')
    val hotSpots = opt[Boolean](short = 'h')
  }

  case class ArgumentsExploration(doDommain: Boolean, doFrequency: Boolean,
                                  doRelativeFrequency: Boolean, doHotSpot: Boolean) extends LineArgumentValuesTrait

}
