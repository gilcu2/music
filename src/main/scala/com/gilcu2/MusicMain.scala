package com.gilcu2

import com.gilcu2.interfaces._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object MusicMain extends MainTrait {

  override val appName = "Traffic"

  override def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    val config = configValues.asInstanceOf[Configuration]
    val arguments = lineArguments.asInstanceOf[Arguments]

    val tracks = Spark.loadCSVFromFile(config.trackPath, delimiter = "\t",header=false)

//    val results=computeTopFromLongestSessions(tracks,arguments.top,arguments.sessions)

  }

  override def getConfigValues(conf: Config): Configuration = {
    val dataPath = conf.getString("DataDir")
    val trackFilename = conf.getString("musicTrack")


    Configuration(dataPath + "/" + trackFilename)
  }

  override def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): Arguments = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val top = parsedArgs.top()
    val sessions = parsedArgs.sessions()

    Arguments(top, sessions)
  }

  case class Configuration(trackPath: String) extends ConfigValuesTrait

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val top = opt[Int](short = 't', default = Some(10))
    val sessions = opt[Int](short = 's', default = Some(50))
  }

  case class Arguments(top: Int, sessions: Int)
    extends LineArgumentValuesTrait

}
