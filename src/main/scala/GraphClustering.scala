import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import PersonalizedPageRank._
import org.apache.spark.internal.Logging
import breeze.linalg.{SparseVector => SV}
import scala.collection.mutable.ArrayBuffer

object GraphClustering extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph Clustering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // load graph
    // *********************************************************************************
    val verticesDataPath = "resources/dblp/test/dblp-vertices.txt"
    val edgesDataPath = "resources/dblp/test/dblp-edges.txt"
    val graph: Graph[(String, Long), Long] = GraphLoader.loadGraph(sc, verticesDataPath, edgesDataPath)
    val hubGraph = graph.subgraph(vpred = (vid, attr) => attr._2 == 0)

    // personalized page rank
    // *********************************************************************************
    val resetProb: Double = 0.2
    val tol: Double = 0.001
    val sources = graph.vertices.filter(v => v._2._2 == 0L).keys.collect()   // 提取主类节点计算ppr
//    sources.sorted.foreach(println(_))

    val personalizedPageRankGraph = PersonalizedPageRank
      .basicParallelPersonalizedPageRank(graph, graph.numVertices.toInt, sources, resetProb, tol)
      .mask(hubGraph)
//    personalizedPageRankGraph.vertices.keys.collect().sorted.foreach(println(_))

    // clustering
    // *********************************************************************************
    val epsilon = 0.005
    val minPts = 3

    val clusteringGraph: Graph[Long, Double] =
      Clustering.clusterGraph(sc, personalizedPageRankGraph, sources, epsilon, minPts)

    // update edge weight
    // *********************************************************************************


    // dblp
//    val edgeTypeNum = 4
//    var initEdgeWeights = Array.fill(edgeTypeNum)(1.0)
//
//    var oldEdgeWeights = initEdgeWeights
//    var newEdgeWeights = initEdgeWeights
  }
}
