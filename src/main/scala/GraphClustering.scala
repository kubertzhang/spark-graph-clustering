import util.Parameters
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import breeze.linalg.{SparseVector => SV}

object GraphClustering extends Logging{
  def main(args: Array[String]): Unit = {
    // for local debug
    val conf = new SparkConf().setAppName("Graph Clustering").setMaster("local[*]")
    val args1 = Array("config/run-parameters.txt", "dataSet=dblp")
    val configFile = args1(0)
    val configParameters = args1(1)

    // for cluster running
//    val conf = new SparkConf().setAppName("Graph Clustering")
//    val configFile = args(0)
//    val configParameters = args(1)

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    println("**************************************************************************")

    // para
    // *********************************************************************************
    val parameters = new Parameters(configFile, configParameters)

    val verticesDataPath = parameters.verticesDataPath
    val edgesDataPath = parameters.edgesDataPath

    val resetProb = parameters.resetProb
    val tol = parameters.tol
    val epsilon = parameters.epsilon
    val minPts = parameters.minPts
    val threshold = parameters.threshold

    val samplingThreshold = parameters.samplingThreshold
    val samplingRate = parameters.samplingRate

    val initialEdgeWeights = parameters.initialEdgeWeights
    var edgeWeights = initialEdgeWeights

    val approach = parameters.approach

    logInfo("**************************************************************************")
    parameters.printParameters()
    println("**************************************************************************")
    logInfo("**************************************************************************")

    // load graph
    // *********************************************************************************
    val originalGraph: Graph[(String, Long), Long] = GraphLoader  // [(vertexCode, vertexTypeId), edgeTypeId]
      .originalGraph(sc, verticesDataPath, edgesDataPath)
//      .partitionBy(PartitionStrategy.EdgePartition2D, 280)
//      .partitionBy(PartitionStrategy.EdgePartition2D)  // partition
      .cache()

//    originalGraph.vertices.collect.filter(_._2._2 == -1L).sorted.foreach(println(_))

    val hubGraph: Graph[(String, Long), Long] = GraphLoader.hubGraph(originalGraph).cache()

    val sources = hubGraph.vertices.keys.collect().sorted  // 提取主类顶点,按编号升序排列,保证每次读取时vertexId顺序一致
    val sourcesBC = sc.broadcast(sources)
//    sourcesBC.value.foreach(println(_))

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
      val clusteringGraph: Graph[Long, Double] =
      Clustering.clusterGraph(sc, personalizedPageRankGraph, epsilon, minPts, optimizedLabel)
      val timeClusteringEnd = System.currentTimeMillis
      personalizedPageRankGraph.unpersist()
      println(s"[result-$approach-clustering running time]: " + (timeClusteringEnd - timeClusteringBegin))
      logInfo(s"[result-$approach-clustering running time]: " + (timeClusteringEnd - timeClusteringBegin))

      // result
      finalClusteringGraph = clusteringGraph

      // edge weight update
      // *********************************************************************************
      val timeUpdateBegin = System.currentTimeMillis
      val edgeWeightUpdateGraph: Graph[(Long, Long), Long] = originalGraph.mapVertices(
        (vid, attr) => (attr._2, -1L)
      )
      .joinVertices(clusteringGraph.vertices){
        (vid, leftAttr, rightAttr) => (leftAttr._1, rightAttr)
      }
      clusteringGraph.unpersist()

      val oldEdgeWeights = edgeWeights
      edgeWeights = EdgeWeightUpdate.updateEdgeWeight(sc, edgeWeightUpdateGraph, edgeWeights)
      val timeUpdateEnd = System.currentTimeMillis
      edgeWeightUpdateGraph.unpersist()
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
