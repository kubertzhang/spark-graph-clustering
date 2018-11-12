import org.apache.spark.graphx._

object EdgeWeightUpdate {
  def updateEdgeWeight(
    edgeWeightUpdateGraph: Graph[(Long, Long), Double],
    oldEdgeWeights: Array[Double]): Array[Double] ={

    val newEdgeWeights = Array(1.0, 1.2, 0.8, 1.0)

    newEdgeWeights
  }
}
