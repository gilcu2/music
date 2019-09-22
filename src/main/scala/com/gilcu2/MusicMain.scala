package com.gilcu2

import com.gilcu2.interfaces._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import com.gilcu2.processing.Processing

object MusicMain extends MainTrait {

  override val appName = "Music"

  override def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    val config = configValues.asInstanceOf[Configuration]
    val arguments = lineArguments.asInstanceOf[Arguments]

    val tracks = Spark.loadCSVFromFile(config.trackPath, delimiter = "\t", header = false, ext = ".tsv")

    val trackLimited = if (arguments.limitRows > 0) {
      logger.info(s"Taking first ${arguments.limitRows} rows")
      tracks.limit(arguments.limitRows)
    } else tracks

    val results = Processing.computeTopSongFromLongestSessions(tracks, arguments.numberOfBiggestSessions, arguments.topNumberOfSongs)

    Spark.saveToCSVFile(results, config.topSongPath, delimiter = "\t", ext = ".tsv", oneFile = true)

  }

  override def getConfigValues(conf: Config): Configuration = {
    val dataPath = conf.getString("DataDir")
    val trackPath = dataPath + "/" + conf.getString("musicTrack")
    val topSongsPath = dataPath + "/" + conf.getString("topSongs")

    Configuration(trackPath, topSongsPath)
  }

  override def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): Arguments = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val top = parsedArgs.top()
    val sessions = parsedArgs.sessions()
    val limit = parsedArgs.limit()

    Arguments(top, sessions, limit)
  }

  case class Configuration(trackPath: String, topSongPath: String) extends ConfigValuesTrait

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val top = opt[Int](short = 't', default = Some(10))
    val sessions = opt[Int](short = 's', default = Some(50))
    val limit = opt[Int](short = 'l', default = Some(0))
  }

  case class Arguments(topNumberOfSongs: Int, numberOfBiggestSessions: Int, limitRows: Int)
    extends LineArgumentValuesTrait

}
