import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import org.apache.spark.internal.Logging
import breeze.linalg.{SparseVector => SV}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.collection.immutable.{Map => immutableMap}


object GraphClustering extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph Clustering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // para
    // *********************************************************************************
    val resetProb: Double = 0.2
    val tol: Double = 0.001
    val epsilon = 0.005
    val minPts = 3

    val initialEdgeWeights = Array(1.0, 1.0, 1.0, 1.0)
    var edgeWeights = Array(1.0, 1.0, 1.0, 1.0)
    val threshold = 0.001
    var mse = Double.MaxValue
    var numIterator = 0
//    val approach = "basic"
    val approach = "incremental"

    // load graph
    // *********************************************************************************
    val verticesDataPath = "resources/dblp/test/dblp-vertices.txt"
    val edgesDataPath = "resources/dblp/test/dblp-edges.txt"
    val originalGraph: Graph[(String, Long), Long] = GraphLoader.originalGraph(sc, verticesDataPath, edgesDataPath)
    val hubGraph: Graph[(String, Long), Long] = GraphLoader.hubGraph(originalGraph)
    val sources = hubGraph.vertices.keys.collect().sorted  // 提取主类顶点,按编号升序排列,保证id和vertexId一致
    val sourcesBC = sc.broadcast(sources)

    val hubPersonalizedPageRankGraph = PersonalizedPageRank
      .hubPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProb, tol)
      .cache()

    while(mse > threshold && (numIterator < 2)){
      // personalized page rank
      // *********************************************************************************
      val personalizedPageRankGraph: Graph[SV[Double], Double] = approach match {
        case "basic" =>
          // 1. basic approach
          PersonalizedPageRank
            .basicPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProb, tol)
            .mask(hubGraph)
          //    personalizedPageRankGraph.vertices.keys.collect().sorted.foreach(println(_))
        case "incremental" =>
          // 2. incremental approach
          PersonalizedPageRank
            .incrementalPersonalizedPageRank(sc, hubPersonalizedPageRankGraph, initialEdgeWeights,
              edgeWeights, sc.broadcast(sourcesBC.value.length), resetProb, tol)
            .mask(hubGraph)
      }

      // clustering
      // *********************************************************************************
      val clusteringGraph: Graph[Long, Double] =
      Clustering.clusterGraph(sc, personalizedPageRankGraph, epsilon, minPts)

      // edge weight update
      // *********************************************************************************
      val oldEdgeWeights = edgeWeights
      edgeWeights = EdgeWeightUpdate.updateEdgeWeight(clusteringGraph, edgeWeights)

      for(i <- edgeWeights.indices){
        mse += math.pow(edgeWeights(i) - oldEdgeWeights(i), 2)
      }
      mse = math.sqrt(mse) / edgeWeights.length

      numIterator += 1
      println(s"[Logging]: graph clustering conduct iteration $numIterator")
      println("**************************************************************************")
    }
    sc.stop()
  }
}
