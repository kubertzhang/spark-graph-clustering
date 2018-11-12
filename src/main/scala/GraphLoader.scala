import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import breeze.linalg.{SparseVector => SV}
import org.apache.spark.broadcast.Broadcast

object GraphLoader {
  def originalGraph(
    sc: SparkContext,
    verticesDataPath: String,
    edgesDataPath: String): Graph[(String, Long), Long] ={

    // load vertices
    val vertexData: RDD[String] = sc.textFile(verticesDataPath)
    val vertices: RDD[(VertexId, (String, Long))] = vertexData.map(
      line => {
        val cols = line.split("\t")
        (cols(0).toLong, (cols(1), cols(2).toLong))
      }
    )

    // load edges
    val edgeData: RDD[String] = sc.textFile(edgesDataPath)
    val edges: RDD[Edge[Long]] = edgeData.map(
      line => {
        val cols = line.split("\t")
        Edge(cols(0).toLong, cols(1).toLong, cols(2).toLong)
      }
    )

    val defaultVertex = ("Missing", -1L)

    Graph(vertices, edges, defaultVertex)
  }

  def hubGraph(graph: Graph[(String, Long), Long]): Graph[(String, Long), Long] ={
    graph.subgraph(vpred = (vid, attr) => attr._2 == 0L)
  }

  def attributeGraph(
    sc: SparkContext,
    graph: Graph[(String, Long), Long],
    edgeWeights: Array[Double],
    sourcesBC: Broadcast[Array[VertexId]]): Graph[(SV[Double], SV[Double], SV[Double], Long), Double] ={

    require(sourcesBC.value.nonEmpty, s"The list of sources must be non-empty," +
      s" but got ${sourcesBC.value.mkString("[", ",", "]")}")

    val sourcesNum = sourcesBC.value.length
    val sourcesNumBC = sc.broadcast(sourcesNum)
    val zeros = SV.zeros[Double](sourcesNumBC.value)
    val totalWeight = edgeWeights.sum

    val totalWeightBC = sc.broadcast(totalWeight)
    val edgeWeightsBC = sc.broadcast(edgeWeights)

    // map of vid -> vector where for each vid, the _position of vid in source_ is set to 1.0
    val sourcesInitMap = sourcesBC.value.zipWithIndex.map {
      case (vid, i) =>
        val estimateVec = SV.zeros[Double](sourcesNumBC.value)
        val residualVec = SV.zeros[Double](sourcesNumBC.value)
        residualVec.update(i, 1.0)
        val maskResidualVec = SV.zeros[Double](sourcesNumBC.value)
        (vid, (estimateVec, residualVec, maskResidualVec))
    }.toMap
    val sourcesInitMapBC = sc.broadcast(sourcesInitMap)

    /* Initialize the attribute graph with:
       each edge transition probability: attribute_weight/(outDegree * total_weight)
       each source vertex with attribute 1.0.
    */
    print("[Logging]: getting attributeGraph: ")
    val attributeGraph = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
      (vid, leftAttr, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets(
      e => edgeWeightsBC.value(e.attr.toString.toInt) / (e.srcAttr * totalWeightBC.value)
    )
      .mapVertices(
        (vid, _) => sourcesInitMapBC.value.getOrElse(vid, (zeros, zeros, zeros))
      )
      // Combine vertex attributes
      .outerJoinVertices(graph.vertices){
      // vertex attr: (estimateVec , residualVec, maskResidualVec, vertexType)
      (vid, leftAttr, rightAttr) => (leftAttr._1, leftAttr._2, leftAttr._3, rightAttr.get._2)
    }
    //    attributeGraph.vertices.collect.foreach(println(_))
    println("done!")

    attributeGraph
  }
}
