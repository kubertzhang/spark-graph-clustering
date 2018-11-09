import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import PersonalizedPageRank._
import org.apache.spark.internal.Logging
import breeze.linalg.{SparseVector => SV}
import scala.collection.mutable.ArrayBuffer

object GraphClustering extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph Clustering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // load graph
    // *********************************************************************************
    // vertex
    val vertexData: RDD[String] = sc.textFile("resources/dblp/test/dblp-vertices.txt")
    val vertices: RDD[(VertexId, (String, Long))] = vertexData.map(
      line => {
        val cols = line.split("\t")
        (cols(0).toLong, (cols(1), cols(2).toLong))
      }
    )

    // edge
    val edgeData: RDD[String] = sc.textFile("resources/dblp/test/dblp-edges.txt")
    val edges: RDD[Edge[Long]] = edgeData.map(
      line => {
        val cols = line.split("\t")
        Edge(cols(0).toLong, cols(1).toLong, cols(2).toLong)
      }
    )

    val defaultVertex = ("Missing", -1L)
    val graph: Graph[(String, Long), Long] = Graph(vertices, edges, defaultVertex)

//    graph.vertices.collect.foreach(println(_))
//    println(graph.vertices.collect.length)
//    println(sources1.length)

    // personalized page rank
    // *********************************************************************************
    val resetProb: Double = 0.2
    val tol: Double = 0.001

    val sources = graph.vertices.filter(v => v._2._2 == 0L).keys.collect()   // 提取主类节点计算ppr
//    println(sources.length)
//    sources.sorted.foreach(println(_))

    val personalizedPageRankGraph = PersonalizedPageRank
      .basicParallelPersonalizedPageRank(graph, graph.numVertices.toInt, sources, resetProb, tol)

    // clustering
    // *********************************************************************************
    val epsilon = 0.005
    val edgeBuffer = ArrayBuffer[List[Int]]()
    personalizedPageRankGraph.vertices.collect.foreach(
      vid_scores => {
        val (vid, scores) = vid_scores
        scores.activeIterator.foreach(
          uid_score =>{
            val (uid, score) = uid_score
            if(score >= epsilon && uid != vid && sources.contains(uid)) {  // 筛选上可以考虑继续优化
//              println(s"vid = $vid, uid = ${uid_score._1}, score = ${uid_score._2}")
              edgeBuffer += List(vid.toInt, uid)
              edgeBuffer += List(uid, vid.toInt)
            }
          }
        )
      }
    )

    val epsilonEdges: RDD[Edge[Double]] = sc.parallelize(edgeBuffer.distinct).map(
      edge => {
        Edge(edge.head, edge(1), 1.0)
      }
    )
//    epsilonEdges.collect.foreach(println(_))
    val minPts = 3
    val minPtsBC = sc.broadcast(minPts)

    val epsilonNeighborGraph = Graph.fromEdges[(Long, Long), Double](epsilonEdges, (-1L, -1L))
    val labeledEpsilonNeighborGraph = epsilonNeighborGraph.outerJoinVertices(epsilonNeighborGraph.outDegrees){
      (vid, attr, deg) => (attr._1, deg.getOrElse(0))
    }
      .mapVertices(
        (vid, attr) => if(attr._2 >= minPtsBC.value) (1L, vid) else (0L, -1L)
      )

//    labeledEpsilonNeighborGraph.vertices.collect.foreach(println(_))

    // dbscan
    // Define functions needed to implement clustering in the GraphX with Pregel
    val initialMessage = -1L

    // vprog func
    def vertexProgram(vid: VertexId, attr: (Long, Long), msg: Long):
    Long = {
      val newAttr = max(attr, msg)
    }


    // dblp
//    val edgeTypeNum = 4
//    var initEdgeWeights = Array.fill(edgeTypeNum)(1.0)
//
//    var oldEdgeWeights = initEdgeWeights
//    var newEdgeWeights = initEdgeWeights


  }
}
