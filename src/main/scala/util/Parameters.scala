package util

//import ConfigLoader

class Parameters(args_ : Array[String])  extends Serializable {
  val configFile = args_ (0)
  val propertyLoader: ConfigLoader = new ConfigLoader(configFile).load()

  val verticesDataPath: String =
    propertyLoader.get("verticesDataPath", "resources/dblp/test/dblp-vertices.txt")
  val edgesDataPath: String =
    propertyLoader.get("edgesDataPath", "resources/dblp/test/dblp-edges.txt")

  val resetProb: Double = propertyLoader.getDouble("resetProb", 0.2)
  val tol: Double = propertyLoader.getDouble("tol", 0.001)

  val epsilon: Double = propertyLoader.getDouble("epsilon", 0.005)
  val minPts: Int = propertyLoader.getInt("minPts", 3)

  val threshold: Double = propertyLoader.getDouble("threshold", 0.001)
  val approach: String = propertyLoader.get("approach", "basic")

  val initialEdgeWeights: Array[Double] =
    propertyLoader.getDoubleArray("initialEdgeWeights", Array(1.0, 1.0, 1.0, 1.0))
}
