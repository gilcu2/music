package com.gilcu2.interfaces

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark {

  def sparkSession(sparkConf: SparkConf): SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  def loadCSVFromFile(path: String, delimiter: String = ",", header: Boolean = true, ext: String = ".csv")(implicit sparkSession: SparkSession): DataFrame = {
    val lines = readTextFile(path + ext)
    loadCSVFromLineDS(lines, delimiter, header)
  }

  def readTextFile(path: String)(implicit spark: SparkSession): Dataset[String] =
    spark.read.textFile(path)

  def loadCSVFromLineDS(lines: Dataset[String], delimiter: String = ",", header: Boolean = true)(implicit sparkSession: SparkSession): DataFrame = {

    sparkSession.read
      .option("header", header)
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .csv(lines)
  }

  def loadCSVFromLineSeq(lines: Seq[String], delimiter: String = ",", header: Boolean = true)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    loadCSVFromLineDS(spark.createDataset(lines), delimiter, header)
  }

  def saveToCSVFile(df: DataFrame, path: String, delimiter: String = ",",
                    header: Boolean = true, ext: String = ".csv",
                    deleteOld: Boolean = true, oneFile: Boolean = false): Unit = {
    val fileName = path + ext
    val tmpFileName = if (oneFile) path + "_tmp" + ext else fileName
    if (deleteOld) {
      HadoopFS.delete(tmpFileName)
      HadoopFS.delete(fileName)
    }

    //    val dfOut = if (oneFile) df.coalesce(1) else df

    df.write
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(tmpFileName)

    if (oneFile) {
      HadoopFS.merge(tmpFileName, fileName)
      HadoopFS.delete(tmpFileName)
    }
  }

  def getTotalCores(implicit spark: SparkSession): Int = {
    //    val executors = spark.sparkContext.statusTracker.getExecutorInfos
    val nExecutors = 1 //executors.size
    val nCores = spark.sparkContext.defaultParallelism
    nExecutors * nCores
  }

}