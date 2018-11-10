import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import PersonalizedPageRank._
import org.apache.spark.internal.Logging
import breeze.linalg.{SparseVector => SV}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

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
    val sources = graph.vertices.filter(v => v._2._2 == 0L).keys.collect()   // 提取主类顶点
//    sources.sorted.foreach(println(_))

    val resetProb: Double = 0.2
    val tol: Double = 0.001
    val epsilon = 0.005
    val minPts = 3

    var newEdgeWeights = Array(1.0, 1.0, 1.0, 1.0)
    val threshold = 0.001
    var mse = Double.MaxValue
    var numIterator = 0
    val hubPersonalizedPageRankGraphMap = Map[String, Graph[(SV[Double], SV[Double]), Double]]()
    val approach = "basic"

    while(mse > threshold && (numIterator < 2)){
      val curEdgeWeights = newEdgeWeights

      // personalized page rank
      // *********************************************************************************
      val personalizedPageRankGraph: Graph[SV[Double], Double] = approach match {
        case "basic" =>
          // 1. basic approach
          PersonalizedPageRank
            .basicParallelPersonalizedPageRank(sc, graph, curEdgeWeights, sources, resetProb, tol)
            .mask(hubGraph)
          //    personalizedPageRankGraph.vertices.keys.collect().sorted.foreach(println(_))
        case "incremental" =>
          // 2. incremental approach
          if(!hubPersonalizedPageRankGraphMap.contains("hub")){
            hubPersonalizedPageRankGraphMap("hub") =
              PersonalizedPageRank.hubPersonalizedPageRank(sc, graph, curEdgeWeights, sources, resetProb, tol)
                .cache()
          }
          PersonalizedPageRank
            .incrementalParallelPersonalizedPageRank(sc, graph, hubPersonalizedPageRankGraphMap("hub"),
              curEdgeWeights, sources, resetProb, tol)
        //case unexpected => Option
      }

      // clustering
      // *********************************************************************************
      val clusteringGraph: Graph[Long, Double] =
      Clustering.clusterGraph(sc, personalizedPageRankGraph, sources, epsilon, minPts)

      // edge weight update
      // *********************************************************************************
      newEdgeWeights = EdgeWeightUpdate.updateEdgeWeight(clusteringGraph, curEdgeWeights)

      for(i <- newEdgeWeights.indices){
        mse += math.pow(newEdgeWeights(i) - curEdgeWeights(i), 2)
      }
      mse = math.sqrt(mse) / newEdgeWeights.length

      numIterator += 1
      println(s"[Logging]: graph clustering conduct iteration $numIterator")
      println("**************************************************************************")
    }
    sc.stop()
  }
}
