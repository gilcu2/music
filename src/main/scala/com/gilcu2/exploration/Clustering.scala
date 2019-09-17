package com.gilcu2.exploration

import com.gilcu2.preprocessing.Preprocessing._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.graphx._

case class CoordinateCount(id: Long, x: Long, y: Long, count: Long)

case class Cluster(coordinates: Seq[CoordinateCount])

object Clustering {

  def findHotSpot(accidents: DataFrame, severity: Int)(implicit spark: SparkSession): Dataset[Cluster] = {
    import spark.implicits._

    val filtered = accidents
      .filter(accidents(locationX).isNotNull && accidents(locationY).isNotNull
        && accidents(accidentSeveriteField) === severity
      )

    val coordinatesAndCount = filtered
      .select(locationX, locationY, accidentIdField)
      .withColumnRenamed(locationX, "x")
      .withColumnRenamed(locationY, "y")
      .withColumnRenamed(accidentIdField, "id")
      .withColumn("count", lit(1))

    val groupedByCoordinate = coordinatesAndCount
      .groupBy("x", "y")
      .agg(sum("count"))
      .withColumnRenamed("sum(count)", "count")
      .withColumn("id", monotonically_increasing_id())
      .as[CoordinateCount]
      .cache()

    groupedByCoordinate.show()

    val conditionUDF = udf[Boolean, Long, Long, Long, Long]((x1, y1, x2, y2) =>
      (x1 < x2 && math.abs(x1 - x2) < 2) || (y1 < y2 && math.abs(y1 - y2) < 2)
    )

    val leftDS = groupedByCoordinate
    val rightDS = groupedByCoordinate
      .select("id", "x", "y")
      .withColumnRenamed("id", "id1")
      .withColumnRenamed("x", "x1")
      .withColumnRenamed("y", "y1")

    val edgesJoin = leftDS
      .join(rightDS, conditionUDF(leftDS("x"), leftDS("y"), rightDS("x1"), rightDS("y1")),
        "cross")

    edgesJoin.show()

    val edges = edgesJoin
      .select("id", "id1")
      .rdd
      .map(r => Edge(r.getLong(r.fieldIndex("id")), r.getLong(r.fieldIndex("id1")), ""))

    val nodes = groupedByCoordinate
      .rdd
      .map(r => (r.id, r))

    val g = Graph(nodes, edges)

    val connectedComponents = g.connectedComponents(10)
    connectedComponents.vertices.collect().foreach(println)

    spark.emptyDataset[Cluster]
  }

  //    Code to log udf
  //    val logLines: CollectionAccumulator[String] = spark.sparkContext.collectionAccumulator("log")
  //
  //    def log(msg: String): Unit = logLines.add(msg)
  //
  //    def writeLog() {
  //      import scala.collection.JavaConverters._
  //      println("start write")
  //      logLines.value.asScala.foreach(println)
  //      println("end write")
  //    }

}
