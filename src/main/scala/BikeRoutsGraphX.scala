import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._

/**
  * Created by ALINA on 16.02.2018.
  */
object BikeRoutsGraphX {

  case class Station()

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val stations_txt = args(0)
    val trips_txt = args(1)

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //Read stations
    val stations = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(stations_txt)

    stations.show()

    //Read trips
    val trips = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(trips_txt)

    trips.show()

    //Create a graph
    val stationVertices = stations.withColumnRenamed("station_id", "id").distinct()
    val tripEdges = trips
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Terminal", "dst")

    val stationGraph = GraphFrame(stationVertices, tripEdges)
    stationGraph.cache()

    stationGraph.triplets.show()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Trips in Original Data: " + trips.count) // sanity check

    val ranks = stationGraph.pageRank.resetProbability(0.15).maxIter(10).run()
    ranks.vertices.orderBy(desc("pagerank")).show()

    //Get the the most common destinations in the dataset from location to location
    val topTrips = stationGraph
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)

    topTrips.show()

    //find the stations with lots of inbound and outbound trips
    val inDeg = stationGraph.inDegrees
    inDeg.orderBy(desc("inDegree")).limit(5).show()

    val outDeg = stationGraph.outDegrees
    outDeg.orderBy(desc("outDegree")).limit(5).show()
  }
}
