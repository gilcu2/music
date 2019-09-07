package com.gilcu2.exploration

import com.gilcu2.interfaces.{ConfigValuesTrait, LineArgumentValuesTrait, MainTrait}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf

object ExplorationMain extends MainTrait {

  def explore(df: DataFrame): Unit = {

    val summary = df.summary()
    summary.show()

  }

  override def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {


  }

  case class ConfigExploration(dataPath: String, filesMap: Map[String, String]) extends ConfigValuesTrait

  override def getConfigValues(conf: Config): ConfigExploration = {
    val dataPath = conf.getString("DataPath")
    val accidentsFile = conf.getString("Accidents")
    val vehiclesFile = conf.getString("Vehicles")
    val caualtiesFile = conf.getString("Casualities")
    ConfigExploration(dataPath)
  }

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val files = trailArg[List[String]](default = Some(List("Accidents")))
  }

  override def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait) = ???

}
