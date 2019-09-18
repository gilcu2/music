package com.gilcu2

import com.gilcu2.exploration.{Clustering, Data, Exploration, Statistic}
import com.gilcu2.interfaces._
import com.gilcu2.preprocessing.Preprocessing
import com.gilcu2.preprocessing.Preprocessing._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopConf

object TrafficMain extends MainTrait {

  override val appName = "Traffic"

  override def process(configValues: ConfigValuesTrait, lineArguments: LineArgumentValuesTrait)(
    implicit spark: SparkSession): Unit = {

    val config = configValues.asInstanceOf[Configuration]
    val arguments = lineArguments.asInstanceOf[Arguments]

    val accidents = Spark.loadCSVFromFile(config.accidentPath)
    val vehicles = Spark.loadCSVFromFile(config.vehiclePath)
    val casualties = Spark.loadCSVFromFile(config.casualtyPath)

    val data = Data(accidents, vehicles, casualties)

    if (arguments.doDommain)
      Exploration.showSummaries(data)

    if (arguments.doFrequency) {
      val fieldsAccidents = Seq(dayOfWeek, lightConditionField, weatherConditionField, roadConditionField)
      Statistic.showSeverityAgainstAccidentFields(data.accidents, arguments.severity, fieldsAccidents)

      val fieldsVehicles = Seq(driverSexField, driverAgeField, ageVehicle, vehicleTypeField)
      Statistic.showSeverityAgainstVehicleFields(data.accidents, data.vehicles, arguments.severity, fieldsVehicles)
    }

    if (arguments.doRelativeFrequency) {
      val fieldsAccidents = Seq(dayOfWeek, lightConditionField, weatherConditionField, roadConditionField)
      Statistic.showRelativeSeverityAgainstAccidentFields(data.accidents, arguments.severity, fieldsAccidents)

      val fieldsVehicles = Seq(driverSexField, driverAgeField, ageVehicle, vehicleTypeField)
      Statistic.showRelativeSeverityAgainstVehicleFields(data.accidents, data.vehicles, arguments.severity, fieldsVehicles)
    }

    if (arguments.doHotSpot) {
      val path = "hotspots.json"

      import spark.implicits._
      val clustering = Clustering.findHotSpots(data.accidents, arguments.severity, minimumAccidents = 2, minimumDistance = 100)
      clustering.cache()
      val nonUnitary = clustering.filter(_.coordinates.size > 1)
      println(s"clusters: ${clustering.count()} non unitary: ${nonUnitary.count()}")
      if (HadoopFS.exists(path))
        HadoopFS.delete(path)
      clustering.toDF().write.json(path)
    }

  }

  def explore(df: DataFrame): Unit = {

    val summary = df.summary()
    summary.show()

  }

  override def getConfigValues(conf: Config): Configuration = {
    val dataPath = conf.getString("DataDir")
    val accidentsFilename = conf.getString("Accidents")
    val vehiclesFilename = conf.getString("Vehicles")
    val casualtiesFilename = conf.getString("Casualties")

    Configuration(
      dataPath + "/" + accidentsFilename,
      dataPath + "/" + vehiclesFilename,
      dataPath + "/" + casualtiesFilename
    )
  }

  override def getLineArgumentsValues(args: Array[String], configValues: ConfigValuesTrait): Arguments = {

    val parsedArgs = new CommandLineParameterConf(args.filter(_.nonEmpty))
    parsedArgs.verify

    val doDomain = parsedArgs.domain()
    val doFrequency = parsedArgs.frequency()
    val doRelativeFrequency = parsedArgs.relativeFrequency()
    val doHotSpot = parsedArgs.hotSpots()
    val severity = parsedArgs.severity()
    val minimumAccidents = parsedArgs.minimumAccidents()
    val minimumDistance = parsedArgs.minimumDistance()

    Arguments(doDomain, doFrequency, doRelativeFrequency, doHotSpot, severity, minimumAccidents, minimumDistance)
  }

  case class Configuration(accidentPath: String, vehiclePath: String, casualtyPath: String) extends ConfigValuesTrait

  class CommandLineParameterConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val domain = opt[Boolean](short = 'o')
    val frequency = opt[Boolean](short = 'f')
    val relativeFrequency = opt[Boolean](short = 'r')
    val hotSpots = opt[Boolean](short = 'h')
    val severity = opt[Int](short = 's', default = Some(1))
    val minimumAccidents = opt[Int](short = 'a', default = Some(2))
    val minimumDistance = opt[Int](short = 'd', default = Some(100))
  }

  case class Arguments(doDommain: Boolean, doFrequency: Boolean,
                       doRelativeFrequency: Boolean, doHotSpot: Boolean,
                       severity: Int, minimumAccidents: Int, minimumDistance: Int)
    extends LineArgumentValuesTrait

}
