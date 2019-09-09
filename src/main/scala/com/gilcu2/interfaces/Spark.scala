package com.gilcu2.interfaces

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark {

  def sparkSession(sparkConf: SparkConf): SparkSession =
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

  def loadCSVFromFile(path: String, delimiter: String = ",", header: Boolean = true)(implicit sparkSession: SparkSession): DataFrame = {
    val lines = readTextFile(path + ".csv")
    loadCSVFromLineDS(lines)
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

  def getTotalCores(implicit spark: SparkSession): Int = {
    //    val executors = spark.sparkContext.statusTracker.getExecutorInfos
    val nExecutors = 1 //executors.size
    val nCores = spark.sparkContext.defaultParallelism
    nExecutors * nCores
  }

}