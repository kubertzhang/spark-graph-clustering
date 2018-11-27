import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.{Map => MutableMap}

object GraphLoader {
  def originalGraph(
    sc: SparkContext,
    verticesDataPath: String,
    edgesDataPath: String): Graph[(String, Long), Long] = {

    // load vertices
    val vertexData: RDD[String] = sc.textFile(verticesDataPath)
    val vertices: RDD[(VertexId, (String, Long))] = vertexData.map(
      line => {
        val cols = line.split("\t")
//        cols.foreach(x => print(s"$x,"))
//        println()
        (cols(0).toLong, (cols(1), cols(2).toLong))
      }
    )
    vertexData.unpersist()

    // load edges
    val edgeData: RDD[String] = sc.textFile(edgesDataPath)
    val edges: RDD[Edge[Long]] = edgeData.map(
      line => {
        val cols = line.split("\t")
//        cols.foreach(x => print(s"$x,"))
//        println()
        Edge(cols(0).toLong, cols(1).toLong, cols(2).toLong)
      }
    )
    edgeData.unpersist()

    val defaultVertex = ("Missing", -1L)

    val originalGraph = Graph(vertices, edges, defaultVertex)
    vertices.unpersist()
    edges.unpersist()

    originalGraph
  }

  def hubGraph(graph: Graph[(String, Long), Long]): Graph[(String, Long), Long] = {
    graph.subgraph(vpred = (vid, attr) => attr._2 == 0L)
  }

  def attributeGraph(
     graph: Graph[(String, Long), Long],
     sourcesBC: Broadcast[Array[VertexId]],
     edgeWeights: Array[Double]):
  Graph[(MutableMap[Long, Double], MutableMap[Long, Double], MutableMap[Long, Double], Long), Double] ={

    require(sourcesBC.value.nonEmpty, s"[error]: sourcePartitionVertices is empty!")

    val totalWeight = edgeWeights.sum
    //    val totalWeightBC = sc.broadcast(totalWeight)
    //    val edgeWeightsBC = sc.broadcast(edgeWeights)

    // map of vid -> vector where for each vid, the _position of vid in source_ is set to 1.0
    val sourcesInitMap = sourcesBC.value.map(
      vid => {
        val estimateMap = MutableMap[Long, Double]()
        val residualMap = MutableMap[Long, Double](vid -> 1.0)
        val maskResidualMap = MutableMap[Long, Double]()
        (vid, (estimateMap, residualMap, maskResidualMap))
      }
    ).toMap
    //    val sourcesInitMapBC = sc.broadcast(sourcesInitMap)

    /* Initialize the attribute graph with:
       each edge transition probability: attribute_weight/(outDegree * total_weight)
       each source vertex with attribute 1.0.
    */
    val attributeGraph = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
      (vid, leftAttr, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets(
      e => edgeWeights(e.attr.toString.toInt) / (e.srcAttr * totalWeight)
    )
      .mapVertices(
        (vid, _) => sourcesInitMap.getOrElse(
          vid, (MutableMap[Long, Double](), MutableMap[Long, Double](), MutableMap[Long, Double]())
        )
      )
      // Combine vertex attributes
      .outerJoinVertices(graph.vertices){
      // vertex attr: (estimateVec , residualVec, maskResidualVec, vertexType)
      (vid, leftAttr, rightAttr) => (leftAttr._1, leftAttr._2, leftAttr._3, rightAttr.get._2)
    }

    attributeGraph
  }
}
