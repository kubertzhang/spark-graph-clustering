import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object GraphLoader {
  def loadGraph(
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
    val graph: Graph[(String, Long), Long] = Graph(vertices, edges, defaultVertex)

    graph
  }
}
