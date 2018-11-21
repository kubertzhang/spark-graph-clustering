import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import breeze.linalg.{SparseVector => SV}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

object Clustering extends Logging {

  def basicClustering(labeledEpsilonNeighborGraph: Graph[(Long, Long), Double]): Graph[Long, Double] ={
    Pregel(
      graph = labeledEpsilonNeighborGraph,
      initialMsg = -1L,
      activeDirection = EdgeDirection.Out)(
      vprog = (vid, attr, msg) => (attr._1, math.max(attr._2, msg)),
      sendMsg = {
        edge => {
          if(edge.srcAttr._1 == 1L && (edge.srcAttr._2 > edge.dstAttr._2)){
            Iterator((edge.dstId, edge.srcAttr._2))
          }
          else{
            Iterator.empty
          }
        }
      },
      mergeMsg = (a, b) => math.max(a, b)
    )
      .mapVertices((_, attr) => attr._2)
  }

  def optimizedClustering(labeledEpsilonNeighborGraph: Graph[(Long, Long), Double]): Graph[Long, Double] = {
    val coreClusteringGraph = {
      Pregel(
        graph = labeledEpsilonNeighborGraph,
        initialMsg = -1L,
        activeDirection = EdgeDirection.Out)(
        vprog = (vid, attr, msg) => (attr._1, math.max(attr._2, msg)),
        //        sendMsg = sendMessage,
        sendMsg = {
          edge => {
            // 只在核心点之间传递消息
            if(edge.srcAttr._1 == 1L && edge.dstAttr._1 == 1L && (edge.srcAttr._2 > edge.dstAttr._2)){
              Iterator((edge.dstId, edge.srcAttr._2))
            }
            else{
              Iterator.empty
            }
          }
        },
        mergeMsg = (a, b) => math.max(a, b)
      )
    }

    Pregel(
      graph = coreClusteringGraph,
      initialMsg = -1L,
      activeDirection = EdgeDirection.Out)(
      vprog = (vid, attr, msg) => (attr._1, math.max(attr._2, msg)),
      sendMsg = {
        edge => {
          // 从核心点向边界点传递消息，并且边界点选择编号最大的簇
          if(edge.srcAttr._1 == 1L && edge.dstAttr._1 == 0L && (edge.srcAttr._2 > edge.dstAttr._2)){
            Iterator((edge.dstId, edge.srcAttr._2))
          }
          else{
            Iterator.empty
          }
        }
      },
      mergeMsg = (a, b) => math.max(a, b)
    )
      .mapVertices((_, attr) => attr._2)
  }

  def clusterGraph(
    sc: SparkContext,
    personalizedPageRankGraph: Graph[SV[Double], Double],
    epsilon: Double = 0.005,
    minPts: Long,
    optimized: Boolean): Graph[Long, Double] ={

    require(epsilon >= 0 && epsilon <= 1, s"Epsilon must belong to [0, 1], but got $epsilon")

    val minPtsBC = sc.broadcast(minPts)

    // 筛选构造符合epsilon条件的主类顶点子图
    val edgeBuffer = ArrayBuffer[List[Int]]()
    personalizedPageRankGraph.vertices.collect.foreach(
      vid_scores => {
        val (vid, scores) = vid_scores
        scores.activeIterator.foreach(
          uid_score =>{
            val (uid, score) = uid_score
            // 筛选主类顶点
            if(score >= epsilon && uid != vid) {
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
    val epsilonNeighborGraph = Graph.fromEdges[(Long, Long), Double](epsilonEdges, (-1L, -1L))

    // 标记核心点
    val labeledEpsilonNeighborGraph = epsilonNeighborGraph.outerJoinVertices(epsilonNeighborGraph.outDegrees){
      (vid, attr, deg) => (attr._1, deg.getOrElse(0))
    }
      .mapVertices(
        (vid, attr) => if(attr._2 >= minPtsBC.value) (1L, vid) else (0L, -1L)  // (vid, (isCorePoint, clusterId))
      )

    // 聚类
    // 核心点和边界点的clusterId > 0L, 离散点的clusterId = -1L
    // Execute a dynamic version of Pregel
    val clusteringGraph = if(optimized){
      basicClustering(labeledEpsilonNeighborGraph)
    }
    else{
      optimizedClustering(labeledEpsilonNeighborGraph)
    }

    require(clusteringGraph.vertices.filter(_._2 > 0L).count() > 0, s"The proper clustered vertices' size" +
      s" must larger than 0, but got 0!")

    clusteringGraph
  }
}
