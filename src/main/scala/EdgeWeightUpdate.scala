import org.apache.spark.graphx._
import scala.math._

object EdgeWeightUpdate {
  def updateEdgeWeight(
    edgeWeightUpdateGraph: Graph[(Long, Long), Long],
    oldEdgeWeights: Array[Double]): Array[Double] = {

    val hubGraph = edgeWeightUpdateGraph.subgraph(vpred = (vid, attr) => attr._1 == 0)
    val hubvertex_num = hubGraph.numVertices
    val clusterid_list = hubGraph.vertices.mapValues((a, b) => b._2).map(x => (x._2, 1L)).reduceByKey(_+_).map(x => x._1).collect()
    var entropyai = Array(0.0, 0.0, 0.0, 0.0)
    var total_entropy = 0.0
    var newweight = new Array[Double](4)

    //println(clusterid_list.length)
    println("oldweight: ")
    for(x <- oldEdgeWeights){
      print(s"$x\t")
    }
    println()

    for (i <- 1 to 3){// number of attributes is 3
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

        val attrvertex : VertexRDD[Double] = edgeWeightUpdateGraph.aggregateMessages[Double]( // num of edges between certain vertex and cluster_k
          triplet => {
            if(triplet.srcAttr._2 == k && triplet.dstAttr._1 == i){
              triplet.sendToDst(1.0)
            }
          },
          (a, b) => a + b
        ).mapValues(x => x / totaledge_num).mapValues(x => -1 * x * log(x))
        val sum = vertex_num_incluster * 1.0 / hubvertex_num * attrvertex.map(x => x._2).reduce((a, b) => a + b)
        entropyaii += sum
        entropyai(i) = entropyaii * 1.0
      }
      total_entropy += entropyai(i)
    }

    for(i <- 1 to 3){
      if(abs(entropyai(i)) <  1e-6){
        entropyai(i) = total_entropy / 3.0
      }
    }

    val inverse_entropy = new Array[Double](4)
    var total_inverse_entropy = 0.0
    for(i<- 1 to 3){/// 333333333
      inverse_entropy(i)= 1.0 / entropyai(i)
      total_inverse_entropy += inverse_entropy(i)
    }

    newweight(0) = 1.0
    for(i <- 1 to 3){
      val bef = oldEdgeWeights(i)
      newweight(i) = (bef + inverse_entropy(i)/ total_inverse_entropy) / 2.0
    }
    println("newweight:" )
    for(x <- newweight){
      print(s"$x\t")
    }
    println()

    newweight
  }
}
