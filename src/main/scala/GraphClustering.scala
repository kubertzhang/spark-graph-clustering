import util.Parameters
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import breeze.linalg.{SparseVector => SV}

object GraphClustering extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph Clustering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // para
    // *********************************************************************************
    val args_ : Array[String] = Array("config/run-parameters.txt")
    val parameters = new Parameters(args_)

    val verticesDataPath = parameters.verticesDataPath
    val edgesDataPath = parameters.edgesDataPath

    val resetProb = parameters.resetProb
    val tol = parameters.tol
    val epsilon = parameters.epsilon
    val minPts = parameters.minPts
    val threshold = parameters.threshold
    val approach = parameters.approach

    val initialEdgeWeights = parameters.initialEdgeWeights
    var edgeWeights = initialEdgeWeights

    // load graph
    // *********************************************************************************
    val originalGraph: Graph[(String, Long), Long] = GraphLoader
      .originalGraph(sc, verticesDataPath, edgesDataPath)
      // partition
//      .partitionBy(PartitionStrategy.EdgePartition2D)
      .cache()
    val hubGraph: Graph[(String, Long), Long] = GraphLoader.hubGraph(originalGraph)
    val sources = hubGraph.vertices.keys.collect().sorted  // 提取主类顶点,按编号升序排列,保证id和vertexId一致
    val sourcesBC = sc.broadcast(sources)

    val hubPersonalizedPageRankGraph = PersonalizedPageRank
      .hubPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProb, tol)
      .cache()

    var mse = Double.MaxValue
    var numIterator = 0
    while(mse > threshold && (numIterator < 1)){
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

//      clusteringGraph.vertices.filter(attr => attr._2 != -1L).collect.foreach(println(_))

      // edge weight update
      // *********************************************************************************
      val edgeWeightUpdateGraph: Graph[(Long, Long), Long] = originalGraph.mapVertices(
        (vid, attr) => (attr._2, -1L)
      )
      .joinVertices(clusteringGraph.vertices){
        (vid, leftAttr, rightAttr) => (leftAttr._1, rightAttr)
      }

//      println("**************************************************************************")
//      edgeWeightUpdateGraph.vertices.filter(attr => attr._2._2 > -1) .collect.foreach(println(_))

      val oldEdgeWeights = edgeWeights
      edgeWeights = EdgeWeightUpdate.updateEdgeWeight(edgeWeightUpdateGraph, edgeWeights)

      // mse
      for(i <- edgeWeights.indices){
        mse += math.pow(edgeWeights(i) - oldEdgeWeights(i), 2)
      }
      mse = math.sqrt(mse) / edgeWeights.length

      numIterator += 1
      println(s"[Logging]: graph clustering conduct iteration $numIterator")
      println("**************************************************************************")
    }

    // clustering metric
    // *********************************************************************************
    sc.stop()
  }
}
