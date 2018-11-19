import util.Parameters
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import breeze.linalg.{SparseVector => SV}

object GraphClustering extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph Clustering").setMaster("local[*]")  // master: local
//    val conf = new SparkConf().setAppName("Graph Clustering")                        // master: cluster
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
//    val approach = parameters.approach
    val approach = "basic"
//    val approach = "incremental"
//    val approach = "reserve"
//    val approach = "sampling"

    val samplingThreshold = 0.001
    val samplingRate = 0.2

    val initialEdgeWeights = parameters.initialEdgeWeights
    var edgeWeights = initialEdgeWeights

//    println(s"parameters: ${parameters.toString}")
//    logInfo(s"parameters: ${parameters.toString}")

    println("**************************************************************************")
    logInfo("**************************************************************************")
    // load graph
    // *********************************************************************************
    val originalGraph: Graph[(String, Long), Long] = GraphLoader
      .originalGraph(sc, verticesDataPath, edgesDataPath)
//      .partitionBy(PartitionStrategy.EdgePartition2D)  // partition
      .cache()
    val hubGraph: Graph[(String, Long), Long] = GraphLoader.hubGraph(originalGraph).cache()
    val sources = hubGraph.vertices.keys.collect().sorted  // 提取主类顶点,按编号升序排列,保证id和vertexId一致
//    val sources = hubGraph.vertices.keys.take(10000)
    val sourcesBC = sc.broadcast(sources)

    // incremental
    var hubPersonalizedPageRankGraph: Graph[(SV[Double], SV[Double], Long), Double] = null
    // reserve
    var dynamicPersonalizedPageRankGraph: Graph[(SV[Double], SV[Double], Array[SV[Double]], Long), Double] = null

    var finalClusteringGraph: Graph[Long, Double] = null

    var mse = Double.MaxValue
    var numIterator = 0
    var optimizedLabel: Boolean = true
    val timeBegin = System.currentTimeMillis
    while(mse > threshold){
      numIterator += 1
      // personalized page rank
      // *********************************************************************************
      val timePPRBegin = System.currentTimeMillis
      val personalizedPageRankGraph: Graph[SV[Double], Double] = approach match {
        case "basic" =>
          // 1. basic approach
          optimizedLabel = false
          PersonalizedPageRank
            .basicPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProb, tol)
            .mask(hubGraph)
          //    personalizedPageRankGraph.vertices.keys.collect().sorted.foreach(println(_))
        case "incremental" =>
          // 2. incremental approach
          optimizedLabel = true

          if(numIterator == 1){
            hubPersonalizedPageRankGraph = PersonalizedPageRank
              .hubPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProb, tol)
              .cache()
          }

          PersonalizedPageRank
            .incrementalPersonalizedPageRank(sc, hubPersonalizedPageRankGraph, initialEdgeWeights,
              edgeWeights, sc.broadcast(sourcesBC.value.length), resetProb, tol)
            .mask(hubGraph)
        case "reserve" =>
          // 3. reserve approach
          optimizedLabel = true

          if(numIterator == 1){
            dynamicPersonalizedPageRankGraph = PersonalizedPageRank
              .dynamicPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProb, tol)
              .cache()
          }
          PersonalizedPageRank
          .reservePersonalizedPageRank(sc, dynamicPersonalizedPageRankGraph, initialEdgeWeights,
            edgeWeights, sc.broadcast(sourcesBC.value.length), resetProb, tol)
            .mask(hubGraph)
        case "sampling" =>
          // 4. sampling approach
          optimizedLabel = true
          val samplingSwitch = true
          PersonalizedPageRank
            .samplingPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProb, tol,
              samplingSwitch, samplingThreshold, samplingRate)
            .mask(hubGraph)
      }
      val timePPREnd = System.currentTimeMillis
      println(s"[result-$approach-ppr running time]: " + (timePPREnd - timePPRBegin))
      logInfo(s"[result-$approach-ppr running time]: " + (timePPREnd - timePPRBegin))

      // clustering
      // *********************************************************************************
      val timeClusteringBegin = System.currentTimeMillis
//      val prevPersonalizedPageRankGraph = personalizedPageRankGraph
      val clusteringGraph: Graph[Long, Double] =
      Clustering.clusterGraph(sc, personalizedPageRankGraph, epsilon, minPts, optimizedLabel)
//      personalizedPageRankGraph.edges.foreachPartition(x => {})  // also materializes personalizedPageRankGraph.vertices
//      prevPersonalizedPageRankGraph.vertices.unpersist(false)
//      prevPersonalizedPageRankGraph.edges.unpersist(false)
//      clusteringGraph.vertices.filter(attr => attr._2 != -1L).collect.foreach(println(_))
      val timeClusteringEnd = System.currentTimeMillis
      println(s"[result-$approach-clustering running time]: " + (timeClusteringEnd - timeClusteringBegin))
      logInfo(s"[result-$approach-clustering running time]: " + (timeClusteringEnd - timeClusteringBegin))

      // result
      finalClusteringGraph = clusteringGraph

      // edge weight update
      // *********************************************************************************
      val timeUpdateBegin = System.currentTimeMillis
//      val prevClusteringGraph = clusteringGraph
      val edgeWeightUpdateGraph: Graph[(Long, Long), Long] = originalGraph.mapVertices(
        (vid, attr) => (attr._2, -1L)
      )
      .joinVertices(clusteringGraph.vertices){
        (vid, leftAttr, rightAttr) => (leftAttr._1, rightAttr)
      }
//      clusteringGraph.edges.foreachPartition(x => {})  // also materializes clusteringGraph.vertices
//      prevClusteringGraph.vertices.unpersist(false)
//      prevClusteringGraph.edges.unpersist(false)
//      edgeWeightUpdateGraph.vertices.filter(attr => attr._2._2 > -1) .collect.foreach(println(_))

//      val prevEdgeWeightUpdateGraph = edgeWeightUpdateGraph
      val oldEdgeWeights = edgeWeights
      edgeWeights = EdgeWeightUpdate.updateEdgeWeight(edgeWeightUpdateGraph, edgeWeights)
//      edgeWeightUpdateGraph.edges.foreachPartition(x => {})
//      prevEdgeWeightUpdateGraph.vertices.unpersist(false)
//      prevEdgeWeightUpdateGraph.edges.unpersist(false)
      val timeUpdateEnd = System.currentTimeMillis
      println(s"[result-$approach-weight update running time]: " + (timeUpdateEnd - timeUpdateBegin))
      logInfo(s"[result-$approach-weight update running time]: " + (timeUpdateEnd - timeUpdateBegin))

      // mse
      mse = 0.0
      for(i <- edgeWeights.indices){
        mse += math.pow(edgeWeights(i) - oldEdgeWeights(i), 2)
      }
      mse = math.sqrt(mse) / edgeWeights.length

      print(s"[result-$approach-$numIterator-edgeWeights]: ")
      logInfo(s"[result-$approach-$numIterator-edgeWeights]: ")
      for(x <- edgeWeights){
        print(s"$x\t")
        logInfo(s"$x\t")
      }
      println()
      logInfo("\n")
      println(s"[result-$approach-$numIterator-mse]: $mse")
      logInfo(s"[result-$approach-$numIterator-mse]: $mse")
      println("**************************************************************************")
      logInfo("**************************************************************************")
    }
    val timeEnd = System.currentTimeMillis
    println(s"[result-$approach-running time]: " + (timeEnd - timeBegin))
    logInfo(s"[result-$approach-running time]: " + (timeEnd - timeBegin))

    // clustering metric & result survey
    // *********************************************************************************
//    finalClusteringGraph.vertices.filter(attr => attr._2 != -1L).collect.foreach(println(_))


    println("**************************************************************************")
    logInfo("**************************************************************************")
    sc.stop()
  }
}
