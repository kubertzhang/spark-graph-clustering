import scala.reflect.ClassTag
import breeze.linalg.{SparseVector => SV}
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.{Vector, Vectors}

object PersonalizedPageRank extends Logging {
  def basicParallelPersonalizedPageRank[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED],
    graphVerticesNum: Int,
    sources: Array[VertexId],
    resetProb: Double = 0.2,
    tol: Double = 0.001): Graph[SV[Double], Double] = {

    require(sources.nonEmpty, s"The list of sources must be non-empty," +
      s" but got ${sources.mkString("[", ",", "]")}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got $resetProb")
    require(tol >= 0 && tol <= 1, s"Tolerance must belong to [0, 1], but got $tol")


    val zeros = SV.zeros[Double](graphVerticesNum)

    // map of vid -> vector where for each vid, the _position of vid in source_ is set to 1.0
    val sourcesInitMap = sources.zipWithIndex.map {
      case (vid, i) =>
        val estimateVec = SV.zeros[Double](graphVerticesNum)
        val residualVec = SV.zeros[Double](graphVerticesNum)
        residualVec.update(i, 1.0)
        val maskResidualVec = SV.zeros[Double](graphVerticesNum)
        (vid, (estimateVec, residualVec, maskResidualVec))
    }.toMap

    val sc = graph.vertices.sparkContext
    val sourcesInitMapBC = sc.broadcast(sourcesInitMap)
    var edgeWeights = Array(1.0, 1.0, 1.0, 1.0)
    val edgeWeightsBC = sc.broadcast(edgeWeights) // sc?
    val totalWeight = 4.0
    val totalWeightBC = sc.broadcast(totalWeight)
    // graph: Graph[(String, Long), Long]

    // Initialize the Personalized PageRank graph with:
    // each edge transition probability: attribute_weight/(outDegree * total_weight)
    // each source vertex with attribute 1.0.
    println("[Logging]: get attributeGraph")
    val attributeGraph = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets(
        e => edgeWeightsBC.value(e.attr.toString.toInt) / (e.srcAttr * totalWeightBC.value)
      )
      .mapVertices(
        (vid, _) => sourcesInitMapBC.value.getOrElse(vid, (zeros, zeros, zeros))
      )

    // Define functions needed to implement Personalized PageRank in the GraphX with Pregel
    // initialMsg
    val initialMessage = zeros

    // vprog func
    def vertexProgram(vid: VertexId, attr: (SV[Double], SV[Double], SV[Double]), msgSumOpt: SV[Double]):
    (SV[Double], SV[Double], SV[Double]) = {
      val oldEstimateVec = attr._1
      val oldResidualVec = attr._2
      val curResidualVec: SV[Double] = oldResidualVec +:+ msgSumOpt

      val maskResVecBuilder = new VectorBuilder[Double](length = -1)
      curResidualVec.activeIterator.filter(kv => kv._2 >= tol).foreach(kv => maskResVecBuilder.add(kv._1, kv._2))
      maskResVecBuilder.length = graphVerticesNum
      val maskResidualVec = maskResVecBuilder.toSparseVector

//      println(maskResidualVec)

      val newEstimateVec = oldEstimateVec +:+ (maskResidualVec *:* resetProb)

//      println(newEstimateVec)

      val newResVecBuilder = new VectorBuilder[Double](length = -1)
      curResidualVec.activeIterator.filter(kv => kv._2 < tol).foreach(kv => newResVecBuilder.add(kv._1, kv._2))
      newResVecBuilder.length = graphVerticesNum
      val newResidualVec = newResVecBuilder.toSparseVector

//      println(newResidualVec)

      (newEstimateVec, newResidualVec, maskResidualVec)
    }

    // sendMsg func
    def sendMessage(edge: EdgeTriplet[(SV[Double], SV[Double], SV[Double]), Double]): Iterator[(VertexId, SV[Double])] = {
      val maskResidualVec = edge.dstAttr._3

      if (maskResidualVec.activeSize != 0) {  // 需要push back
        val msgSumOpt = maskResidualVec *:* edge.attr *:* (1.0 - resetProb)
        Iterator((edge.srcId, msgSumOpt))
      } else {
        Iterator.empty
      }
    }

    // mergeMsg func
    def mergeMessage(a: SV[Double], b: SV[Double]): SV[Double] = a +:+ b

    // Execute a dynamic version of Pregel
    println("[Logging]: run Pregel func ... ")
    val personalizedPageRankGraph = Pregel(
      graph = attributeGraph,
      initialMsg = initialMessage,
      activeDirection = EdgeDirection.In)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage)
      .mapVertices((_, attr) => attr._1)

//    personalizedPageRankGraph.vertices.collect.foreach(println(_))

    personalizedPageRankGraph
  }
}
