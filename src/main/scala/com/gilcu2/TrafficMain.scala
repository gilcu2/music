package com.gilcu2

import com.gilcu2.exploration.{Data, Exploration, Statistic}
import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, MainTrait, Spark}
import com.gilcu2.preprocessing.Preprocessing
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf
import Preprocessing._

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
      val fields = Seq(driverAgeField, lightConditionField, weatherConditionField, roadConditionField)
      Statistic.showSeverityAgaintsFields(accidentVehicles, severity = 1, fields)
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

    ArgumentsExploration(doDomain, doFrequency)
  }

  case class ConfigExploration(accidentPath: String, vehiclePath: String, casualtyPath: String) extends ConfigValuesTrait

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val domain = opt[Boolean](short = 'd')
    val frequency = opt[Boolean](short = 'f')
  }

  case class ArgumentsExploration(doDommain: Boolean, doFrequency: Boolean) extends LineArgumentValuesTrait

}
