import breeze.linalg.{SparseVector => SV}
import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.broadcast.Broadcast

object PersonalizedPageRank {
  def runParallelPersonalizedPageRank(
    sc: SparkContext,
    attributeGraph: Graph[(SV[Double], SV[Double], SV[Double], Long), Double],
    sourcesNumBC: Broadcast[Int],
    pushBackType: String,
    resetProb: Double = 0.2,
    tol: Double = 0.001): Graph[(SV[Double], SV[Double], SV[Double], Long), Double] = {

    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got $resetProb")
    require(tol >= 0 && tol <= 1, s"Tolerance must belong to [0, 1], but got $tol")

    val zeros = SV.zeros[Double](sourcesNumBC.value)

    // Define functions needed to implement Personalized PageRank in the GraphX with Pregel
    // initialMsg
    val initialMessage = zeros

    // Partial random walk
    def hubVertexProgram(vid: VertexId, attr: (SV[Double], SV[Double], SV[Double], Long), msgSumOpt: SV[Double]):
    (SV[Double], SV[Double], SV[Double], Long) = {
      val oldEstimateVec = attr._1
      val oldResidualVec = attr._2
      val vertexType = attr._4
      val curResidualVec: SV[Double] = oldResidualVec +:+ msgSumOpt

      val maskResVecBuilder = new VectorBuilder[Double](length = -1)
      // 只push back主类顶点: vertexType == 0
      curResidualVec.activeIterator.filter(kv => (vertexType == 0) && (kv._2 >= tol))
        .foreach(kv => maskResVecBuilder.add(kv._1, kv._2))
      maskResVecBuilder.length = sourcesNumBC.value
      val maskResidualVec = maskResVecBuilder.toSparseVector

      val newEstimateVec = oldEstimateVec +:+ (maskResidualVec *:* resetProb)

      val newResVecBuilder = new VectorBuilder[Double](length = -1)
      // 只push back主类顶点, 属性类顶点的residual value保留
      curResidualVec.activeIterator.filter(kv => (vertexType != 0) || (kv._2 < tol))
        .foreach(kv => newResVecBuilder.add(kv._1, kv._2))
      newResVecBuilder.length = sourcesNumBC.value
      val newResidualVec = newResVecBuilder.toSparseVector

      (newEstimateVec, newResidualVec, maskResidualVec, vertexType)
    }

    // Global random walk
    def globalVertexProgram(vid: VertexId, attr: (SV[Double], SV[Double], SV[Double], Long), msgSumOpt: SV[Double]):
    (SV[Double], SV[Double], SV[Double], Long) = {
      val oldEstimateVec = attr._1
      val oldResidualVec = attr._2
      val curResidualVec: SV[Double] = oldResidualVec +:+ msgSumOpt

      val maskResVecBuilder = new VectorBuilder[Double](length = -1)
      curResidualVec.activeIterator.filter(kv => kv._2 >= tol).foreach(kv => maskResVecBuilder.add(kv._1, kv._2))
      maskResVecBuilder.length = sourcesNumBC.value
      val maskResidualVec = maskResVecBuilder.toSparseVector

      val newEstimateVec = oldEstimateVec +:+ (maskResidualVec *:* resetProb)

      val newResVecBuilder = new VectorBuilder[Double](length = -1)
      curResidualVec.activeIterator.filter(kv => kv._2 < tol).foreach(kv => newResVecBuilder.add(kv._1, kv._2))
      newResVecBuilder.length = sourcesNumBC.value
      val newResidualVec = newResVecBuilder.toSparseVector

      (newEstimateVec, newResidualVec, maskResidualVec, attr._4)
    }

    // vprog func
    def vertexProgram(vid: VertexId, attr: (SV[Double], SV[Double], SV[Double], Long), msgSumOpt: SV[Double]):
    (SV[Double], SV[Double], SV[Double], Long) = {
      if(pushBackType == "partial"){
        hubVertexProgram(vid, attr, msgSumOpt)
      }
      else { // global
        globalVertexProgram(vid, attr, msgSumOpt)
      }
    }

    // sendMsg func
    def sendMessage(edge: EdgeTriplet[(SV[Double], SV[Double], SV[Double], Long), Double]):
    Iterator[(VertexId, SV[Double])] = {
      val maskResidualVec = edge.dstAttr._3

      if (maskResidualVec.activeSize != 0) {  // 存在需要push back的顶点,继续发送消息
        val msgSumOpt = maskResidualVec *:* edge.attr *:* (1.0 - resetProb)
        Iterator((edge.srcId, msgSumOpt))
      } else {
        Iterator.empty
      }
    }

    // mergeMsg func
    def mergeMessage(a: SV[Double], b: SV[Double]): SV[Double] = a +:+ b

    // Execute a dynamic version of Pregel
    val personalizedPageRankGraph = Pregel(
      graph = attributeGraph,
      initialMsg = initialMessage,
      activeDirection = EdgeDirection.In)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage
      )
//    personalizedPageRankGraph.vertices.collect.foreach(println(_))

    personalizedPageRankGraph  // (vid, (estimateVec , residualVec, maskResidualVec, vertexType))
  }

  def basicPersonalizedPageRank(
    sc: SparkContext,
    originalGraph: Graph[(String, Long), Long],
    sourcesBC: Broadcast[Array[VertexId]],
    edgeWeights: Array[Double],
    resetProb: Double = 0.2,
    tol: Double = 0.001): Graph[SV[Double], Double] = {

    val attributeGraph = GraphLoader.attributeGraph(sc, originalGraph, edgeWeights, sourcesBC)
    val sourcesNumBC = sc.broadcast(sourcesBC.value.length)
    runParallelPersonalizedPageRank(sc, attributeGraph, sourcesNumBC, "global", resetProb, tol)
      .mapVertices((_, attr) => attr._1)  // (vid, (estimateVec))
  }

  def hubPersonalizedPageRank(
    sc: SparkContext,
    originalGraph: Graph[(String, Long), Long],
    sourcesBC: Broadcast[Array[VertexId]],
    edgeWeights: Array[Double],
    resetProb: Double = 0.2,
    tol: Double = 0.001): Graph[(SV[Double], SV[Double], Long), Double] = {

    val attributeGraph = GraphLoader.attributeGraph(sc, originalGraph, edgeWeights, sourcesBC)
    val sourcesNumBC = sc.broadcast(sourcesBC.value.length)
    runParallelPersonalizedPageRank(sc, attributeGraph, sourcesNumBC, "partial", resetProb, tol)
      .mapVertices((_, attr) => (attr._1, attr._2, attr._4))  // (vid, (estimateVec , residualVec, vertexType))
  }

  def incrementalPersonalizedPageRank(
    sc: SparkContext,
    hubPersonalizedPageRankGraph: Graph[(SV[Double], SV[Double], Long), Double],
    oldEdgeWeights: Array[Double],
    newEdgeWeights: Array[Double],
    sourcesNumBC: Broadcast[Int],
    resetProb: Double = 0.2,
    tol: Double = 0.001): Graph[SV[Double], Double] = {

    val zeros = SV.zeros[Double](sourcesNumBC.value)
    val zerosBC = sc.broadcast(zeros)
    val oldEdgeWeightsBC = sc.broadcast(oldEdgeWeights)
    val newEdgeWeightsBC = sc.broadcast(newEdgeWeights)

    val updatedAttributeGraph = hubPersonalizedPageRankGraph.mapVertices(
      (_, attr) => {
        val (estimateVec, residualVec, vertexType) = attr
        if(vertexType != 0L){
          val newResidualVec =
            residualVec *:* (newEdgeWeightsBC.value(vertexType.toInt) / oldEdgeWeightsBC.value(vertexType.toInt)
          )
          (estimateVec, newResidualVec, zerosBC.value, vertexType)
        }
        else{
          (estimateVec, residualVec, zerosBC.value, vertexType)
        }
      }
    )

    runParallelPersonalizedPageRank(sc, updatedAttributeGraph, sourcesNumBC, "global", resetProb, tol)
      .mapVertices((_, attr) => attr._1)  // (vid, (estimateVec))
  }

  def reservePersonalizedPageRank(
    sc: SparkContext,
    originalGraph: Graph[(String, Long), Long],
    sourcesBC: Broadcast[Array[VertexId]],
    edgeWeights: Array[Double],
    resetProb: Double = 0.2,
    tol: Double = 0.001): Graph[(SV[Double], SV[Double], Array[SV[Double]], Long), Double] = {

    val attributeGraph = GraphLoader.attributeGraph(sc, originalGraph, edgeWeights, sourcesBC)
    val sourcesNumBC = sc.broadcast(sourcesBC.value.length)
    val zeros = SV.zeros[Double](sourcesNumBC.value)
    val zerosBC = sc.broadcast(zeros)
    val attributeNum = edgeWeights.length - 1
    val attributeNumBC = sc.broadcast(attributeNum)
    val reserveWeight = (1 - resetProb) / (resetProb * edgeWeights.sum)
    val reserveWeightBC = sc.broadcast(reserveWeight)

    val basicPersonalizedPageRankGraph =
    runParallelPersonalizedPageRank(sc, attributeGraph, sourcesNumBC, "global", resetProb, tol)
      .mapVertices(
        (_, attr) => {
          val reserveVecArray = Array.fill(attributeNumBC.value)(zerosBC.value)
          (attr._1, attr._2, SV.zeros[Int](attributeNumBC.value), reserveVecArray, attr._4)
        }
      )  // (vid, (estimateVec , residualVec, reserveVecArray, vertexType))

    // initialMsg
    val initialMessage = (SV.zeros[Int](attributeNumBC.value), Array.fill(attributeNumBC.value)(zerosBC.value))

    // vprog func
    def vertexProgram(vid: VertexId, attr: (SV[Double], SV[Double], SV[Int], Array[SV[Double]], Long),
      msgSumOpt: (SV[Int], Array[SV[Double]])): (SV[Double], SV[Double], SV[Int], Array[SV[Double]], Long) = {
      (attr._1, attr._2, msgSumOpt._1, msgSumOpt._2, attr._5)
    }

    // sendMsg func
    def sendMessage(edge: EdgeTriplet[(SV[Double], SV[Double], SV[Int], Array[SV[Double]], Long), Double]):
    Iterator[(VertexId, (SV[Int], Array[SV[Double]]))] = {
      val srcVertexType = edge.srcAttr._5
      val dstVertexType = edge.dstAttr._5
      if(srcVertexType != 0L && dstVertexType == 0L){
        val countSV = SV.zeros[Int](attributeNumBC.value)
        val attributeIndex = (srcVertexType - 1).toInt
        countSV.update(attributeIndex, 1)
        val reserveVecArray = Array.fill(attributeNumBC.value)(zerosBC.value)
        reserveVecArray(attributeIndex) = edge.srcAttr._1
        Iterator((edge.dstId, (countSV, reserveVecArray)))
      }
      else {
        Iterator.empty
      }
    }

    // mergeMsg func
    def mergeMessage(a: (SV[Int], Array[SV[Double]]), b: (SV[Int], Array[SV[Double]])):
    (SV[Int], Array[SV[Double]]) = {
      val msg: Array[SV[Double]] = Array.fill(a._2.length)(zerosBC.value)
      for(i <- a._2.indices){
        msg(i) = a._2(i) +:+ b._2(i)
      }
      (a._1 +:+ b._1, msg)
    }

    // Execute a dynamic version of Pregel
    val reservePersonalizedPageRankGraph = Pregel(
      graph = basicPersonalizedPageRankGraph,
      initialMsg = initialMessage,
      maxIterations = 1,  // 只处理邻居顶点
      activeDirection = EdgeDirection.Out)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage
      )
      .mapVertices(
        (_, attr) => {
          val countSV = attr._3
          val reserveVecArray = attr._4
          for(i <- 0 until attributeNumBC.value){
            reserveVecArray(i) = reserveVecArray(i) *:* ((1.0 / countSV(i)) * reserveWeightBC.value)
          }
          (attr._1, attr._2, reserveVecArray, attr._5)
        }
      )

    reservePersonalizedPageRankGraph
  }

  def approximatePersonalizedPageRank(
    sc: SparkContext,
    reservePersonalizedPageRankGraph: Graph[(SV[Double], SV[Double], Array[SV[Double]], Long), Double],
    oldEdgeWeights: Array[Double],
    newEdgeWeights: Array[Double],
    sourcesNumBC: Broadcast[Int],
    resetProb: Double = 0.2,
    tol: Double = 0.001): Graph[SV[Double], Double] = {

    val zeros = SV.zeros[Double](sourcesNumBC.value)
    val zerosBC = sc.broadcast(zeros)
    val oldEdgeWeightsBC = sc.broadcast(oldEdgeWeights)
    val newEdgeWeightsBC = sc.broadcast(newEdgeWeights)
    val attributeNum = newEdgeWeights.length - 1
    val attributeNumBC = sc.broadcast(attributeNum)

    val updatedAttributeGraph = reservePersonalizedPageRankGraph.mapVertices(
      (_, attr) => {
        val (estimateVec, residualVec, reserveVecArray, vertexType) = attr
        if(vertexType == 0L){
          var newResidualVec = residualVec
          for(i <- 0 until attributeNumBC.value){
            newResidualVec =
              newResidualVec +:+ (reserveVecArray(i) *:* (newEdgeWeightsBC.value(i) - oldEdgeWeightsBC.value(i)))
          }
          (estimateVec, newResidualVec, zerosBC.value, vertexType)
        }
        else{
          (estimateVec, residualVec, zerosBC.value, vertexType)
        }
      }
    )

    runParallelPersonalizedPageRank(sc, updatedAttributeGraph, sourcesNumBC, "global", resetProb, tol)
      .mapVertices((_, attr) => attr._1)  // (vid, (estimateVec))
  }

}
