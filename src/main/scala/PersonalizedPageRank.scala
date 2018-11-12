import scala.reflect.ClassTag
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
//      println(maskResidualVec)

      val newEstimateVec = oldEstimateVec +:+ (maskResidualVec *:* resetProb)
//      println(newEstimateVec)

      val newResVecBuilder = new VectorBuilder[Double](length = -1)
      // 只push back主类顶点: vertexType == 0, 属性类顶点的residual value保留
      curResidualVec.activeIterator.filter(kv => (vertexType != 0) || (kv._2 < tol))
        .foreach(kv => newResVecBuilder.add(kv._1, kv._2))
      newResVecBuilder.length = sourcesNumBC.value
      val newResidualVec = newResVecBuilder.toSparseVector
//      println(newResidualVec)

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
//      println(maskResidualVec)

      val newEstimateVec = oldEstimateVec +:+ (maskResidualVec *:* resetProb)
//      println(newEstimateVec)

      val newResVecBuilder = new VectorBuilder[Double](length = -1)
      curResidualVec.activeIterator.filter(kv => kv._2 < tol).foreach(kv => newResVecBuilder.add(kv._1, kv._2))
      newResVecBuilder.length = sourcesNumBC.value
      val newResidualVec = newResVecBuilder.toSparseVector
//      println(newResidualVec)

      (newEstimateVec, newResidualVec, maskResidualVec, attr._4)
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

    // mergeMsg func
    def mergeMessage(a: SV[Double], b: SV[Double]): SV[Double] = a +:+ b

    // Execute a dynamic version of Pregel
    print("[Logging]: getting personalizedPageRankGraph: ")
    val personalizedPageRankGraph = Pregel(
      graph = attributeGraph,
      initialMsg = initialMessage,
      activeDirection = EdgeDirection.In)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage
    )
    println("done!")
//    personalizedPageRankGraph.vertices.collect.foreach(println(_))

    personalizedPageRankGraph
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
      .mapVertices((_, attr) => attr._1)
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
      .mapVertices((_, attr) => (attr._1, attr._2, attr._4))
  }

  def incrementalPersonalizedPageRank[VD: ClassTag, ED: ClassTag](
    sc: SparkContext,
    hubPersonalizedPageRankGraph: Graph[(SV[Double], SV[Double], Long), Double],
    oldEdgeWeights: Array[Double],
    newEdgeWeights: Array[Double],
    sourcesNumBC: Broadcast[Int],
    resetProb: Double = 0.2,
    tol: Double = 0.001): Graph[SV[Double], Double] = {

    val zeros = SV.zeros[Double](sourcesNumBC.value)
    val oldEdgeWeightsBC = sc.broadcast(oldEdgeWeights)
    val newEdgeWeightsBC = sc.broadcast(newEdgeWeights)

    val updatedAttributeGraph = hubPersonalizedPageRankGraph.mapVertices(
      (vid, attr) => {
        val (estimateVec, residualVec, vertexType) = attr
        if(vertexType != 0L){
          val newResidualVec = residualVec *:* (newEdgeWeightsBC.value(vertexType.toInt)
            / oldEdgeWeightsBC.value(vertexType.toInt)
          )
          (estimateVec, newResidualVec, zeros, vertexType)
        }
        else{
          (estimateVec, residualVec, zeros, vertexType)
        }
      }
    )

    runParallelPersonalizedPageRank(sc, updatedAttributeGraph, sourcesNumBC, "global", resetProb, tol)
      .mapVertices((_, attr) => attr._1)
  }
}
