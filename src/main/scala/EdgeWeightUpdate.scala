import org.apache.spark.graphx._
import breeze.linalg.{SparseVector => SV}

import scala.math._

object EdgeWeightUpdate {

  def updateEdgeWeight(
    edgeWeightUpdateGraph: Graph[(Long, Long), Long],  // [(vertexTypeId, clusterId), edgeTypeId]
    oldEdgeWeights: Array[Double]): Array[Double] = {

    val attributeNum = oldEdgeWeights.length - 1

//    val vertexNumByTypeArray = new Array[Long](attributeNum + 1)

    val vertexNumByTypeArray = edgeWeightUpdateGraph.vertices.groupBy(_._2._1).map(
      kv => (kv._1, kv._2.count(_ => true))
    ).sortBy(_._1).map(_._2)

    val countFrequencyGraph = edgeWeightUpdateGraph.mapVertices(
      (_, attr) => {
        val attributeArray = vertexNumByTypeArray.map(
          num => SV.zeros[Long](num)
        )
        (attr._1, attr._2, attributeArray)
      }
    )



    oldEdgeWeights
  }


  def updateEdgeWeightT(
    edgeWeightUpdateGraph: Graph[(Long, Long), Long],
    oldEdgeWeights: Array[Double]): Array[Double] = {

    val attr_num = oldEdgeWeights.length - 1
    val hubGraph = edgeWeightUpdateGraph.subgraph(vpred = (vid, attr) => attr._2 > 0)
    val hubvertex_num = hubGraph.numVertices
    val clusterid_list = hubGraph.vertices.map(x => x._2._2).collect().toSet.filter(x => x > 0)
    var entropyai = new Array[Double](attr_num + 1)
    var total_entropy = 0.0
    val newweight = new Array[Double](attr_num + 1)

    for (i <- 1 to attr_num){
      //val vertexcount = graph.vertices.map(x => x._2._1 == i).count()
      for (k <- clusterid_list){
        var entropyaii = 0.0
        val vertex_num_incluster = hubGraph.vertices.filter(x => x._2._2 == k).count()
        var totaledge_num : Long = 0 // total number of the edges between cluster_k and attribute_i

        val alledges : VertexRDD[Long] = edgeWeightUpdateGraph.aggregateMessages[Long](
          triplet => {
            if(triplet.srcAttr._1 == i && triplet.dstAttr._2 == k){
              triplet.sendToDst(1L)
            }
          },
          (a,b) => a + b
        )
        totaledge_num = alledges.map(x => x._2).reduce(_+_)
        //println ("totaledge_num:" + totaledge_num)

        // num of edges between certain vertex and cluster_k
        val attrvertex : VertexRDD[Double] = edgeWeightUpdateGraph.aggregateMessages[Double](
          triplet => {
            if(triplet.srcAttr._2 == k && triplet.dstAttr._1 == i){
              triplet.sendToDst(1.0)
            }
          },
          (a, b) => a + b
        ).mapValues(x => -1 * x / totaledge_num * log(x / totaledge_num))
        //mapValues(x => x / totaledge_num).mapValues(x => -1 * x * log(x))
        val sum = vertex_num_incluster * 1.0 / hubvertex_num * attrvertex.map(x => x._2).reduce((a, b) => a + b)
        entropyaii += sum
        entropyai(i) = entropyaii * 1.0
      }
      total_entropy += entropyai(i)
    }

    for(i <- 1 to attr_num){
      if(abs(entropyai(i)) <  1e-6){
        entropyai(i) = 1.0 * total_entropy / attr_num
      }
    }

    val inverse_entropy = new Array[Double](attr_num + 1)
    var total_inverse_entropy = 0.0
    for(i<- 1 to attr_num){
      inverse_entropy(i)= 1.0 / entropyai(i)
      total_inverse_entropy += inverse_entropy(i)
    }

    newweight(0) = 1.0
    for(i <- 1 to attr_num){
      val bef = oldEdgeWeights(i)
      newweight(i) = (bef + inverse_entropy(i)/ total_inverse_entropy * attr_num) / 2.0
    }

    newweight
  }
}
