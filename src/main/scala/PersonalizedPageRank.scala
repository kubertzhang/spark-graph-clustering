import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.broadcast.Broadcast
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{Map => MutableMap}

object PersonalizedPageRank {
  def runParallelPersonalizedPageRank(
    attributeGraph: Graph[(MutableMap[Long, Double], MutableMap[Long, Double], MutableMap[Long, Double], Long), Double],
    pushBackType: String,
    resetProbBC: Broadcast[Double],
    tolBC: Broadcast[Double],
    samplingSwitch: Boolean = false,
    samplingThreshold: Double = 0.001,
    samplingRate: Double = 0.2):
    Graph[(MutableMap[Long, Double], MutableMap[Long, Double], MutableMap[Long, Double], Long), Double] = {

    require(resetProbBC.value >= 0 && resetProbBC.value <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProbBC.value}")
    require(tolBC.value >= 0 && tolBC.value <= 1, s"Tolerance must belong to [0, 1], but got ${tolBC.value}")

    // initialMsg
    val initialMessage = MutableMap[Long, Double]()

    // Partial random walk
    def hubVertexProgram(vid: VertexId, attr: (MutableMap[Long, Double], MutableMap[Long, Double],
        MutableMap[Long, Double], Long), msgSumOpt: MutableMap[Long, Double]): (MutableMap[Long, Double],
        MutableMap[Long, Double], MutableMap[Long, Double], Long) = {

      val curResidualMap = (attr._2 /: msgSumOpt)(
        (map, kv) => { map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0))) }
      )

      // 只push back主类顶点(vertexType == 0L)
      val propagateResidualMap = curResidualMap.filter(kv => (attr._4 == 0L) && (kv._2 >= tolBC.value))

      val newEstimateMap = (attr._1 /: curResidualMap)(
        (map, kv) => { map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0) * resetProbBC.value)) }
      )

      // 只push back主类顶点, 属性类顶点的residual value保留
      val newResidualMap = curResidualMap.filter(kv => (attr._4 != 0L) || (kv._2 < tolBC.value))

      (newEstimateMap, newResidualMap, propagateResidualMap, attr._4)
    }

    // Global random walk
    def globalVertexProgram(vid: VertexId, attr: (MutableMap[Long, Double], MutableMap[Long, Double],
        MutableMap[Long, Double], Long), msgSumOpt: MutableMap[Long, Double]): (MutableMap[Long, Double],
        MutableMap[Long, Double], MutableMap[Long, Double], Long) = {

      val curResidualMap = (attr._2 /: msgSumOpt)(
        (map, kv) => { map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0))) }
      )
      val propagateResidualMap = curResidualMap.filter(_._2 >= tolBC.value)
      val newEstimateMap = (attr._1 /: curResidualMap)(
        (map, kv) => { map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0) * resetProbBC.value)) }
      )
      val newResidualMap = curResidualMap.filter(_._2 < tolBC.value)
//      println(s"$vid: ${newEstimateMap.size}, ${newResidualMap.size}, ${propagateResidualMap.size}")

      (newEstimateMap, newResidualMap, propagateResidualMap, attr._4)
    }

    // vprog func
    def vertexProgram(vid: VertexId, attr: (MutableMap[Long, Double], MutableMap[Long, Double],
        MutableMap[Long, Double], Long), msgSumOpt: MutableMap[Long, Double]): (MutableMap[Long, Double],
        MutableMap[Long, Double], MutableMap[Long, Double], Long) = {

      if(pushBackType == "partial"){
        hubVertexProgram(vid, attr, msgSumOpt)
      }
      else { // global
        globalVertexProgram(vid, attr, msgSumOpt)
      }
    }

    // normal send operation
    def normalSendMessage(edge: EdgeTriplet[(MutableMap[Long, Double], MutableMap[Long, Double],
        MutableMap[Long, Double], Long), Double]): Iterator[(VertexId, MutableMap[Long, Double])] = {

      val propagateResidualMap = edge.dstAttr._3
      if (propagateResidualMap.nonEmpty) {  // 存在需要push back的顶点,继续发送消息
        val msgSumOpt = collection.mutable.Map(
          propagateResidualMap.mapValues(x => x * edge.attr *:* (1.0 - resetProbBC.value))
            .toSeq: _*
        )
        Iterator((edge.srcId, msgSumOpt))
      } else {
        Iterator.empty
      }
    }

    // sampling send operation
    def samplingSendMessage(edge: EdgeTriplet[(MutableMap[Long, Double], MutableMap[Long, Double],
        MutableMap[Long, Double], Long), Double]): Iterator[(VertexId, MutableMap[Long, Double])] = {

      val propagateResidualMap = edge.dstAttr._3

      if(propagateResidualMap.nonEmpty){  // 存在需要push back的顶点,继续发送消息
        // sampling
        if(edge.attr < samplingThreshold){  // 如果入边转移概率小于采样阈值,进行采样
          val randomValue = new Random().nextInt(10).toDouble
          if(randomValue < samplingRate * 10){  // 采样保留
            val msgSumOpt = collection.mutable.Map(
              propagateResidualMap.mapValues(x => x * edge.attr *:* (1.0 - resetProbBC.value))
                .toSeq: _*
            )
            Iterator((edge.srcId, msgSumOpt))
          } else { Iterator.empty }  // 采样丢弃
        }
        else {  // 入边转移概率大于采样阈值,不进行采样
          val msgSumOpt = collection.mutable.Map(
            propagateResidualMap.mapValues(x => x * edge.attr *:* (1.0 - resetProbBC.value))
              .toSeq: _*
          )
          Iterator((edge.srcId, msgSumOpt))
        }
      }else{
        Iterator.empty
      }
    }

    // sendMsg func
    def sendMessage(edge: EdgeTriplet[(MutableMap[Long, Double], MutableMap[Long, Double], MutableMap[Long, Double],
      Long), Double]): Iterator[(VertexId, MutableMap[Long, Double])] = {
      if(samplingSwitch){
        samplingSendMessage(edge)
      }
      else{
        normalSendMessage(edge)
      }
    }

    // mergeMsg func
    def mergeMessage(a: MutableMap[Long, Double], b: MutableMap[Long, Double]): MutableMap[Long, Double] = {
      (a /: b)(
        (map, kv) => { map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0))) }
      )
    }

    // Execute a dynamic version of Pregel
    val personalizedPageRankGraph = Pregel(
      graph = attributeGraph,
      initialMsg = initialMessage,
      maxIterations = 3,
      activeDirection = EdgeDirection.In)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage
      )

    personalizedPageRankGraph  // (vid, (estimateMap , residualMap, propagateResidualMap, vertexType))
  }

  def basicPersonalizedPageRank(
    originalGraph: Graph[(String, Long), Long],
    sourcesBC: Broadcast[Array[VertexId]],
    edgeWeights: Array[Double],
    resetProbBC: Broadcast[Double],
    tolBC: Broadcast[Double]): Graph[MutableMap[Long, Double], Double] = {

    val attributeGraph = GraphLoader.attributeGraph(originalGraph, sourcesBC, edgeWeights)
    val basicPersonalizedPageRankGraph = runParallelPersonalizedPageRank(attributeGraph, pushBackType = "global",
      resetProbBC, tolBC)
      .mapVertices((_, attr) => attr._1)  // (vid, (estimateMap))
    attributeGraph.unpersist()

    basicPersonalizedPageRankGraph
  }

  def hubPersonalizedPageRank(
      originalGraph: Graph[(String, Long), Long],
      sourcesBC: Broadcast[Array[VertexId]],
      edgeWeights: Array[Double],
      resetProbBC: Broadcast[Double],
      tolBC: Broadcast[Double]): Graph[(MutableMap[Long, Double], MutableMap[Long, Double], Long), Double] = {

    val attributeGraph = GraphLoader.attributeGraph(originalGraph, sourcesBC, edgeWeights)
    val hubPersonalizedPageRankGraph = runParallelPersonalizedPageRank(attributeGraph, pushBackType = "partial",
      resetProbBC, tolBC)
      .mapVertices((_, attr) => (attr._1, attr._2, attr._4))  // (vid, (estimateMap , residualMap, vertexType))
    attributeGraph.unpersist()

    hubPersonalizedPageRankGraph
  }

  def incrementalPersonalizedPageRank(
    hubPersonalizedPageRankGraph: Graph[(MutableMap[Long, Double], MutableMap[Long, Double], Long), Double],
    oldEdgeWeightsBC: Broadcast[Array[Double]],
    newEdgeWeights: Array[Double],
    resetProbBC: Broadcast[Double],
    tolBC: Broadcast[Double]): Graph[MutableMap[Long, Double], Double] = {

    val attributeGraph = hubPersonalizedPageRankGraph.mapVertices(
      (_, attr) => {
        val vertexType = attr._3
        if(vertexType != 0L){ // 更新属性顶点
          val updateRatio = newEdgeWeights(vertexType.toInt) / oldEdgeWeightsBC.value(vertexType.toInt)
          val newResidualMap = collection.mutable.Map(
            attr._2.mapValues(x => x * updateRatio).toSeq: _*
          )
          (attr._1, newResidualMap, MutableMap[Long, Double](), vertexType)
        }
        else{
          (attr._1, attr._2, MutableMap[Long, Double](), vertexType)
        }
      }
    )

    val incrementalPersonalizedPageRankGraph = runParallelPersonalizedPageRank(attributeGraph, pushBackType = "global",
      resetProbBC, tolBC)
      .mapVertices((_, attr) => attr._1)  // (vid, (estimateMap))
    attributeGraph.unpersist()

    incrementalPersonalizedPageRankGraph
  }

  def dynamicPersonalizedPageRank(
      sc: SparkContext,
      originalGraph: Graph[(String, Long), Long],
      sourcesBC: Broadcast[Array[VertexId]],
      edgeWeights: Array[Double],
      resetProbBC: Broadcast[Double],
      tolBC: Broadcast[Double]): Graph[(MutableMap[Long, Double], MutableMap[Long, Double],
      Array[MutableMap[Long, Double]], Long), Double] = {

    val attributeNum = edgeWeights.length - 1
    val attributeNumBC = sc.broadcast(attributeNum)
    val reserveWeight = (1 - resetProbBC.value) / (resetProbBC.value * edgeWeights.sum)
    val reserveWeightBC = sc.broadcast(reserveWeight)

    val attributeGraph = GraphLoader.attributeGraph(originalGraph, sourcesBC, edgeWeights)

    val dynamicAttributeGraph =
      runParallelPersonalizedPageRank(attributeGraph, pushBackType = "global", resetProbBC, tolBC)
      .mapVertices(
        (_, attr) => {
          val reserveMapArray = Array.fill(attributeNumBC.value)(MutableMap[Long, Double]())
          (attr._1, attr._2, new Array[Int](attributeNumBC.value), reserveMapArray, attr._4)
        }
      )  // (vid, (estimateMap , residualMap, reserveMapArray, vertexType))
    attributeGraph.unpersist()

    // initialMsg
    val initialMessage =
      (new Array[Int](attributeNumBC.value), Array.fill(attributeNumBC.value)(MutableMap[Long, Double]()))

    // vprog func
    def vertexProgram(vid: VertexId, attr: (MutableMap[Long, Double], MutableMap[Long, Double], Array[Int],
        Array[MutableMap[Long, Double]], Long), msgSumOpt: (Array[Int], Array[MutableMap[Long, Double]])):
        (MutableMap[Long, Double], MutableMap[Long, Double], Array[Int], Array[MutableMap[Long, Double]], Long) = {

      val time1 = System.currentTimeMillis
      val newCounts = attr._3 +:+ msgSumOpt._1
      for(i <- attr._4.indices){
        attr._4(i) = (attr._4(i) /: msgSumOpt._2(i))(
          (map, kv) => { map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0))) }
        )
      }
      val time2 = System.currentTimeMillis
      println("vp = " + (time2 - time1))

      (attr._1, attr._2, newCounts, attr._4, attr._5)
    }

    // sendMsg func
    def sendMessage(edge: EdgeTriplet[(MutableMap[Long, Double], MutableMap[Long, Double], Array[Int],
        Array[MutableMap[Long, Double]], Long), Double]): Iterator[(VertexId, (Array[Int],
        Array[MutableMap[Long, Double]]))] = {

      val time1 = System.currentTimeMillis
      val srcVertexType = edge.srcAttr._5
      val dstVertexType = edge.dstAttr._5

      if((srcVertexType != 0L) && (dstVertexType == 0L) && (edge.attr > 0.05)){
//        val countArray = new Array[Int](attributeNumBC.value)
//        val attributeIndex = (srcVertexType - 1).toInt
//        countArray.update(attributeIndex, 1)
//        val reserveMapArray = Array.fill(attributeNumBC.value)(MutableMap[Long, Double]())
//        reserveMapArray(attributeIndex) = edge.srcAttr._1
        // -------------------------------------------------

        val attributeIndex = (srcVertexType - 1).toInt
        edge.srcAttr._3.update(attributeIndex, 1)
        edge.srcAttr._4(attributeIndex) = edge.srcAttr._1

        // -------------------------------------------------

        val time2 = System.currentTimeMillis
        println("send = " + (time2 - time1))
        Iterator((edge.dstId, (edge.srcAttr._3, edge.srcAttr._4)))
      }
      else {
        val time2 = System.currentTimeMillis
        println("send = " + (time2 - time1))
        Iterator.empty
      }
    }

    // mergeMsg func
    def mergeMessage(a: (Array[Int], Array[MutableMap[Long, Double]]), b: (Array[Int],
        Array[MutableMap[Long, Double]])): (Array[Int], Array[MutableMap[Long, Double]]) = {

      val time1 = System.currentTimeMillis
      for(i <- a._2.indices){
        a._2(i) = (a._2(i) /: b._2(i))(
          (map, kv) => { map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0))) }
        )
      }

      val time2 = System.currentTimeMillis
      println("merge = " + (time2 - time1))
      (a._1 +:+ b._1, a._2)
    }

    // Execute a dynamic version of Pregel
    val dynamicPersonalizedPageRankGraph = Pregel(
      graph = dynamicAttributeGraph,
      initialMsg = initialMessage,
      maxIterations = 1,  // 只处理邻居顶点
      activeDirection = EdgeDirection.Out)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage
      )
      .mapVertices(
        (_, attr) => {
          val countArray = attr._3
          val reserveMapArray = attr._4
          for(i <- 0 until attributeNumBC.value){
            val reserveRatio = (1.0 / countArray(i)) * reserveWeightBC.value
            reserveMapArray(i).mapValues(x => x * reserveRatio)

          }
          (attr._1, attr._2, reserveMapArray, attr._5)
        }
      )

    dynamicPersonalizedPageRankGraph
  }

  def reservePersonalizedPageRank(
    sc: SparkContext,
    dynamicPersonalizedPageRankGraph: Graph[(MutableMap[Long, Double], MutableMap[Long, Double],
      Array[MutableMap[Long, Double]], Long), Double],
    oldEdgeWeightsBC: Broadcast[Array[Double]],
    newEdgeWeights: Array[Double],
    resetProbBC: Broadcast[Double],
    tolBC: Broadcast[Double]): Graph[MutableMap[Long, Double], Double] = {

    val attributeNum = newEdgeWeights.length - 1
    val attributeNumBC = sc.broadcast(attributeNum)

    val updatedAttributeGraph = dynamicPersonalizedPageRankGraph.mapVertices(
      (_, attr) => {
        val (estimateMap, residualMap, reserveMapArray, vertexType) = attr
        if(vertexType == 0L){
          var newResidualMap = residualMap
          for(i <- 0 until attributeNumBC.value){
            reserveMapArray(i).mapValues(x => x * (newEdgeWeights(i) - oldEdgeWeightsBC.value(i))).filter(_._2 > 0.0)
            newResidualMap = (newResidualMap /: reserveMapArray(i))(
              (map, kv) => { map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0))) }
            )
          }
          (estimateMap, newResidualMap, MutableMap[Long, Double](), vertexType)
        }
        else{
          (estimateMap, residualMap, MutableMap[Long, Double](), vertexType)
        }
      }
    )

    val reservePersonalizedPageRankGraph = runParallelPersonalizedPageRank(updatedAttributeGraph,
      pushBackType = "global", resetProbBC, tolBC)
      .mapVertices((_, attr) => attr._1)  // (vid, (estimateMap))
    updatedAttributeGraph.unpersist()

    reservePersonalizedPageRankGraph
  }

  def samplingPersonalizedPageRank(
      originalGraph: Graph[(String, Long), Long],
      sourcesBC: Broadcast[Array[VertexId]],
      edgeWeights: Array[Double],
      resetProbBC: Broadcast[Double],
      tolBC: Broadcast[Double],
      samplingSwitch: Boolean = true,  // 默认开启
      samplingThreshold: Double = 0.001,
      samplingRate: Double = 0.2): Graph[MutableMap[Long, Double], Double] = {

    val attributeGraph = GraphLoader.attributeGraph(originalGraph, sourcesBC, edgeWeights)
    val samplingPersonalizedPageRankGraph = runParallelPersonalizedPageRank(attributeGraph,
      pushBackType = "global", resetProbBC, tolBC, samplingSwitch, samplingThreshold, samplingRate)
      .mapVertices((_, attr) => attr._1)  // (vid, (estimateMap))
    attributeGraph.unpersist()

    samplingPersonalizedPageRankGraph
  }

}
