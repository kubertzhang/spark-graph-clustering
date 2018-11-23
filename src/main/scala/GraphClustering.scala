import util.Parameters
import util.ClusteringMetric
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import breeze.linalg.{SparseVector => SV}

object GraphClustering extends Logging{
  def main(args: Array[String]): Unit = {
    // for local debug
//    val conf = new SparkConf().setAppName("Graph Clustering").setMaster("local[*]")
//    val args1 = Array("config/run-parameters.txt", "dataSet=dblp")
//    val configFile = args1(0)
//    val configParameters = args1(1)

    // for cluster running
    val conf = new SparkConf().setAppName("Graph Clustering")
    val configFile = args(0)
    val configParameters = args(1)

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
    var edgeWeights: Array[Double] = initialEdgeWeights

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

    // test
//    originalGraph.vertices.filter(_._2._2 == -1L).foreach(println(_))

//    originalGraph.vertices.collect.filter(_._2._2 == -1L).sorted.foreach(println(_))

    val hubGraph: Graph[(String, Long), Long] = GraphLoader.hubGraph(originalGraph).cache()

    val sources = hubGraph.vertices.keys.collect().sorted  // 提取主类顶点,按编号升序排列,保证每次读取时vertexId顺序一致
    val sourcesBC = sc.broadcast(sources)
//    sourcesBC.value.foreach(println(_))

    // incremental
    var hubPersonalizedPageRankGraph: Graph[(SV[Double], SV[Double], Long), Double] = null
    // reserve
    var dynamicPersonalizedPageRankGraph: Graph[(SV[Double], SV[Double], Array[SV[Double]], Long), Double] = null

    var finalClusteringGraph: Graph[(Long, Long), Long] = null

    var mse = Double.MaxValue
    var numIteration = 0
    var optimizedLabel: Boolean = true
    var PreProcessingTime: Long = 0L
    val timeBegin = System.currentTimeMillis
    while(mse > threshold){
      numIteration += 1
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

          if(numIteration == 1){
            val incrementalTimeBegin = System.currentTimeMillis
            hubPersonalizedPageRankGraph = PersonalizedPageRank
              .hubPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProb, tol)
              .cache()
            val incrementalTimeEnd = System.currentTimeMillis
            PreProcessingTime = incrementalTimeEnd - incrementalTimeBegin
          }

          PersonalizedPageRank
            .incrementalPersonalizedPageRank(sc, hubPersonalizedPageRankGraph, initialEdgeWeights,
              edgeWeights, sc.broadcast(sourcesBC.value.length), resetProb, tol)
            .mask(hubGraph)
        case "reserve" =>
          // 3. reserve approach
          optimizedLabel = true

          if(numIteration == 1){
            val reserveTimeBegin = System.currentTimeMillis
            dynamicPersonalizedPageRankGraph = PersonalizedPageRank
              .dynamicPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProb, tol)
              .cache()
            val reserveTimeEnd = System.currentTimeMillis
            PreProcessingTime = reserveTimeEnd - reserveTimeBegin
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

//      println(s"s = ${clusteringGraph.vertices.filter(_._2 > 0L).count()}")

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

      // result
      finalClusteringGraph = edgeWeightUpdateGraph

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

      print(s"[result-$approach-$numIteration-edgeWeights]: ")
      logInfo(s"[result-$approach-$numIteration-edgeWeights]: ")
      for(x <- edgeWeights){
        print(s"$x\t")
        logInfo(s"$x\t")
      }
      println()
      logInfo("\n")
      println(s"[result-$approach-$numIteration-mse]: $mse")
      logInfo(s"[result-$approach-$numIteration-mse]: $mse")
      println("**************************************************************************")
      logInfo("**************************************************************************")
    }
    val timeEnd = System.currentTimeMillis

    // clustering metric & result survey
    // *********************************************************************************
    println("**************************************************************************")
    logInfo("**************************************************************************")
    println("*************** distributed graph clustering results *********************")
    logInfo("*************** distributed graph clustering results *********************")
    
    // parameters
    parameters.printParameters()
    
    // running time
    println(s"[result-total running time]: " + (timeEnd - timeBegin - PreProcessingTime))
    logInfo(s"[result-total running time]: " + (timeEnd - timeBegin - PreProcessingTime))
    
    // edge weights
    print(s"[result-final edgeWeights]: ")
    logInfo(s"[result-final edgeWeights]: ")
    for(x <- edgeWeights){
      print(s"$x\t")
      logInfo(s"$x\t")
    }
    println()
    
    // iteration times
    println(s"[result-iteration times]: $numIteration")

    // clustering quality
    val density = ClusteringMetric.density(finalClusteringGraph)
    val entropy = ClusteringMetric.entropy(sc, finalClusteringGraph, edgeWeights)
    println(s"[result-density]: $density")
    println(s"[result-entropy]: $entropy")
    logInfo(s"[result-density]: $density")
    logInfo(s"[result-entropy]: $entropy")

    println("**************************************************************************")
    logInfo("**************************************************************************")
    sc.stop()
  }
}
