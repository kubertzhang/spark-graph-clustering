import util.Parameters
import util.ClusteringMetric
import util.ParameterSelection
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.collection.mutable.{Map => MutableMap}

object GraphClustering extends Logging{
  def main(args: Array[String]): Unit = {
    // for local debug
//    val conf = new SparkConf().setAppName("Graph Clustering").setMaster("local[*]")
//    val args1 = Array("config/run-parameters.txt", "dataSet=dblp,dataSize=1m")
//    val configFile = args1(0)
//    val configParameters = args1(1)

    // for cluster running
    val conf = new SparkConf().setAppName("Graph Clustering")
    val configFile = args(0)
    val configParameters = args(1)

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    println("[RESULT]**************************************************************************")

    // para
    // *********************************************************************************
    val parameters = new Parameters(configFile, configParameters)

    val verticesDataPath = parameters.verticesDataPath
    val edgesDataPath = parameters.edgesDataPath

    val resetProbBC = sc.broadcast(parameters.resetProb)
    val tolBC = sc.broadcast(parameters.tol)
    val epsilonBC = sc.broadcast(parameters.epsilon)
    val minPtsBC = sc.broadcast(parameters.minPts)
    val threshold = parameters.threshold

    val initialEdgeWeights = parameters.initialEdgeWeights
    var edgeWeights = initialEdgeWeights
    val initialEdgeWeightsBC = sc.broadcast(initialEdgeWeights)

    val samplingThreshold = parameters.samplingThreshold
    val samplingRate = parameters.samplingRate

    val approach = parameters.approach

    parameters.printParameters()
    println("[RESULT]**************************************************************************")

    // load graph
    // *********************************************************************************
    val originalGraph: Graph[(String, Long), Long] = GraphLoader  // [(vertexCode, vertexTypeId), edgeTypeId]
      .originalGraph(sc, verticesDataPath, edgesDataPath)
//      .partitionBy(PartitionStrategy.EdgePartition2D, 280)  // (partition, partitionNum)
//      .partitionBy(PartitionStrategy.EdgePartition2D)  // partition
      .cache()

//    originalGraph.vertices.collect.foreach(println(_))

    val hubGraph: Graph[(String, Long), Long] = GraphLoader.hubGraph(originalGraph).cache()

    // 提取主类顶点
    val sources = hubGraph.vertices.keys.collect()
    val sourcesBC = sc.broadcast(sources)

    // incremental
    var hubPersonalizedPageRankGraph: Graph[(MutableMap[Long, Double], MutableMap[Long, Double], Long), Double] = null
    // reserve
    var dynamicPersonalizedPageRankGraph: Graph[(MutableMap[Long, Double], MutableMap[Long, Double],
      Array[MutableMap[Long, Double]], Long), Double] = null

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
      val personalizedPageRankGraph: Graph[MutableMap[Long, Double], Double] = approach match {
        case "basic" =>
          // 1. basic approach
          optimizedLabel = false
          PersonalizedPageRank
            .basicPersonalizedPageRank(originalGraph, sourcesBC, edgeWeights, resetProbBC, tolBC)
            .mask(hubGraph)
        case "incremental" =>
          // 2. incremental approach
          optimizedLabel = true

          // 预处理
          if(numIteration == 1){
            val incrementalTimeBegin = System.currentTimeMillis
            hubPersonalizedPageRankGraph = PersonalizedPageRank
              .hubPersonalizedPageRank(originalGraph, sourcesBC, edgeWeights, resetProbBC, tolBC)
              .cache()
            val incrementalTimeEnd = System.currentTimeMillis
            PreProcessingTime = incrementalTimeEnd - incrementalTimeBegin
          }

          PersonalizedPageRank
            .incrementalPersonalizedPageRank(hubPersonalizedPageRankGraph, initialEdgeWeightsBC, edgeWeights,
              resetProbBC, tolBC)
            .mask(hubGraph)
        case "reserve" =>
          // 3. reserve approach
          optimizedLabel = true

          // 预处理
          if(numIteration == 1){
            val reserveTimeBegin = System.currentTimeMillis
            dynamicPersonalizedPageRankGraph = PersonalizedPageRank
              .dynamicPersonalizedPageRank(sc, originalGraph, sourcesBC, edgeWeights, resetProbBC, tolBC)
              .cache()
            val reserveTimeEnd = System.currentTimeMillis
            PreProcessingTime = reserveTimeEnd - reserveTimeBegin
          }
          PersonalizedPageRank
          .reservePersonalizedPageRank(sc, dynamicPersonalizedPageRankGraph, initialEdgeWeightsBC,
            edgeWeights, resetProbBC, tolBC)
            .mask(hubGraph)
        case "sampling" =>
          // 4. sampling approach
          optimizedLabel = true
          val samplingSwitch = true
          PersonalizedPageRank
            .samplingPersonalizedPageRank(originalGraph, sourcesBC, edgeWeights, resetProbBC, tolBC,
              samplingSwitch, samplingThreshold, samplingRate)
            .mask(hubGraph)
      }
      val timePPREnd = System.currentTimeMillis
      println(s"[RESULT][result-$approach-ppr running time]: " + (timePPREnd - timePPRBegin))

      // parameter selection
      // *********************************************************************************
//      val EpsilonAndMinPtsMap = ParameterSelection.selectEpsilonAndMinPts(sc, personalizedPageRankGraph)
//      EpsilonAndMinPtsMap.toArray.sortBy(_._1).foreach(
//        kv => {
//          kv._2.toArray.sortBy(_._1).foreach(
//            kv2 => println(s"[RESULT]epsilon=${kv._1}: ${kv2._1}-${kv2._2}")
//          )
//        }
//      )
//      require(false)

      // clustering
      // *********************************************************************************
      val timeClusteringBegin = System.currentTimeMillis
      val clusteringGraph: Graph[Long, Double] =
      Clustering.clusterGraph(sc, personalizedPageRankGraph, epsilonBC, minPtsBC, optimizedLabel)
      val timeClusteringEnd = System.currentTimeMillis
      personalizedPageRankGraph.unpersist()
      println(s"[RESULT][result-$approach-clustering running time]: " + (timeClusteringEnd - timeClusteringBegin))

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
      println(s"[RESULT][result-$approach-weight update running time]: " + (timeUpdateEnd - timeUpdateBegin))

      // mse
      mse = 0.0
      for(i <- edgeWeights.indices){
        mse += math.pow(edgeWeights(i) - oldEdgeWeights(i), 2)
      }
      mse = math.sqrt(mse) / edgeWeights.length

      print(s"[RESULT][result-$approach-$numIteration-edgeWeights]: ")
      for(x <- edgeWeights){
        print(s"$x\t")
      }
      println()
      println(s"[RESULT][result-$approach-$numIteration-mse]: $mse")
      println("[RESULT]**************************************************************************")
    }
    val timeEnd = System.currentTimeMillis

    // clustering metric & result survey
    // *********************************************************************************
    println("[RESULT]**************************************************************************")
    println("[RESULT]*************** distributed graph clustering results *********************")
    
    // parameters
    parameters.printParameters()
    
    // running time
    println(s"[RESULT][result-total running time]: " + (timeEnd - timeBegin - PreProcessingTime))
    
    // edge weights
    print(s"[RESULT][result-final edgeWeights]: ")
    for(x <- edgeWeights){
      print(s"$x\t")
      logInfo(s"$x\t")
    }
    println()
    
    // iteration times
    println(s"[RESULT][result-iteration times]: $numIteration")

    // clustering quality
    val clusteredSourcesSize = finalClusteringGraph.vertices.filter(_._2._2 > 0L).count()
    val density = ClusteringMetric.density(finalClusteringGraph)
    val entropy = ClusteringMetric.entropy(sc, finalClusteringGraph, edgeWeights)
    println(s"[RESULT][result-sources size]: ${sourcesBC.value.length}")
    println(s"[RESULT][result-clustered vertices size]: $clusteredSourcesSize")
    println(s"[RESULT][result-density]: $density")
    println(s"[RESULT][result-entropy]: $entropy")

    println("[RESULT]**************************************************************************")
    sc.stop()
  }
}
