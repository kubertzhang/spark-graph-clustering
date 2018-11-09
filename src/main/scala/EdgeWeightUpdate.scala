import org.apache.spark.graphx._
import scala.reflect.ClassTag

object EdgeWeightUpdate {
  def updateEdgeWeight[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED],
    oldEdgeWeights: Array[Double]): Array[Double] ={

    val newEdgeWeights = Array(1.0, 1.2, 0.8, 1.0)

    newEdgeWeights
  }
}
