package com.gilcu2

import com.gilcu2.exploration.{Data, Exploration, Statistic}
import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, MainTrait, Spark}
import com.gilcu2.preprocessing.Preprocessing
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

    if (argumentsExploration.doAgeSeverity) {
      val r = Statistic.computeSeverityDriverAge(accidentVehicles, severity = 1)
      r.show()
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
    val doAgeSeverity = parsedArgs.ageSeverity()

    ArgumentsExploration(doDomain, doAgeSeverity)
  }

  case class ConfigExploration(accidentPath: String, vehiclePath: String, casualtyPath: String) extends ConfigValuesTrait

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val domain = opt[Boolean](short = 'o')
    val ageSeverity = opt[Boolean](short = 'a')
  }

  case class ArgumentsExploration(doDommain: Boolean, doAgeSeverity: Boolean) extends LineArgumentValuesTrait

}
