import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import breeze.linalg.{SparseVector => SV}
import org.apache.spark.SparkContext

object Clustering {
  def clusterGraph(
    sc: SparkContext,
    personalizedPageRankGraph: Graph[SV[Double], Double],
    epsilon: Double = 0.005,
    minPts: Long): Graph[Long, Double] ={

    require(epsilon >= 0 && epsilon <= 1, s"Epsilon must belong to [0, 1], but got $epsilon")

    val minPtsBC = sc.broadcast(minPts)

    val edgeBuffer = ArrayBuffer[List[Int]]()
    personalizedPageRankGraph.vertices.collect.foreach(
      vid_scores => {
        val (vid, scores) = vid_scores
        scores.activeIterator.foreach(
          uid_score =>{
            val (uid, score) = uid_score
            // 筛选主类顶点
            if(score >= epsilon && uid != vid) {  // 筛选上可以考虑继续优化
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

    val epsilonNeighborGraph = Graph.fromEdges[(Long, Long), Double](epsilonEdges, (-1L, -1L))
    val labeledEpsilonNeighborGraph = epsilonNeighborGraph.outerJoinVertices(epsilonNeighborGraph.outDegrees){
      (vid, attr, deg) => (attr._1, deg.getOrElse(0))
    }
      .mapVertices(
        (vid, attr) => if(attr._2 >= minPtsBC.value) (1L, vid) else (0L, -1L)
      )
    //    labeledEpsilonNeighborGraph.vertices.collect.sorted.foreach(println(_))

    // sendMsg func
    def sendMessage(edge: EdgeTriplet[(Long, Long), Double]): Iterator[(VertexId, Long)] = {
      if(edge.srcAttr._1 == 1L && (edge.srcAttr._2 > edge.dstAttr._2)){
        Iterator((edge.dstId, edge.srcAttr._2))
      }
      else{
        Iterator.empty
      }
    }

    // Execute a dynamic version of Pregel
    val clusteringGraph = Pregel(
      graph = labeledEpsilonNeighborGraph,
      initialMsg = -1L,
      activeDirection = EdgeDirection.Out)(
        vprog = (vid, attr, msg) => (attr._1, math.max(attr._2, msg)),
        sendMsg = sendMessage,
        mergeMsg = (a, b) => math.max(a, b)
    )
      .mapVertices((_, attr) => attr._2)
//    clusteringGraph.vertices.collect.sorted.foreach(println(_))

    clusteringGraph
  }
}
