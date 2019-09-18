package com.gilcu2.exploration

import com.gilcu2.preprocessing.Preprocessing._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class CoordinateCount(id: Long, x: Long, y: Long, count: Long)

case class Cluster(id: Long, coordinates: Seq[CoordinateCount])

object Clustering {

  def findHotSpots(accidents: DataFrame, severity: Int, minimumAccidents: Int, minimumDistance: Int, maxIterations: Int = 10)(implicit spark: SparkSession): RDD[Cluster] = {
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
      .filter(_.count >= minimumAccidents)
      .cache()

    println("Accidents per point sample")
    groupedByCoordinate.show()

    val conditionUDF = udf[Boolean, Long, Long, Long, Long]((x1, y1, x2, y2) =>
      (x1 < x2 && math.abs(x1 - x2) < minimumDistance) || (y1 < y2 && math.abs(y1 - y2) < minimumDistance)
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

    println("Near points sample")
    edgesJoin.show()

    val edges = edgesJoin
      .select("id", "id1")
      .rdd
      .map(r => Edge(r.getLong(r.fieldIndex("id")), r.getLong(r.fieldIndex("id1")), ""))
      .cache()

    val nodes = groupedByCoordinate
      .rdd
      .map(r => (r.id, r))
      .cache()

    val g = Graph(nodes, edges)

    val connectedComponents = g.connectedComponents(maxIterations)

    val clusterIds = connectedComponents.vertices

    val clustering = clusterIds
      .innerJoin(g.vertices) { case (nodeId, clusterId, coordinateCount) =>
        (clusterId, coordinateCount)
      }
      .groupBy(_._2._1)
      .map { case (nodeId, clusterSeq) =>
        Cluster(clusterSeq.head._1,
          clusterSeq.map(_._2._2).toSeq)
      }

    clustering
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
