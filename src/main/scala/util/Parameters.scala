package util

//import ConfigLoader

class Parameters(configFile: String, configParameters: String)  extends Serializable {
  val propertyLoader: ConfigLoader = new ConfigLoader(configFile, configParameters).load().update()

  val resetProb: Double = propertyLoader.getDouble("resetProb", 0.2)
  val tol: Double = propertyLoader.getDouble("tol", 0.001)
  val threshold: Double = propertyLoader.getDouble("threshold", 0.001)

  val dataSet: String = propertyLoader.get("dataSet", "[warning: you need to set dataSet]")
  val dataSize: String = propertyLoader.get("dataSize", "[warning: you need to set dataSize]")

  val verticesDataPath: String =
    propertyLoader.get("verticesDataPath", "[warning: you need to set verticesDataPath]")
  val edgesDataPath: String =
    propertyLoader.get("edgesDataPath", "[warning: you need to set edgesDataPath]")

  val epsilon: Double = propertyLoader.getDouble("epsilon", 0.005)
  val minPts: Int = propertyLoader.getInt("minPts", 3)

  val initialEdgeWeights: Array[Double] =
    propertyLoader.getDoubleArray("initialEdgeWeights", Array(1.0, 1.0, 1.0, 1.0))

  val samplingThreshold: Double = propertyLoader.getDouble("samplingThreshold", 0.001)
  val samplingRate: Double = propertyLoader.getDouble("samplingRate", 0.2)

  val approach: String = propertyLoader.get("approach", "basic")

  def printParameters(): Unit ={
    println(s"[RESULT]Parameters: {resetProb=$resetProb, tol=$tol, threshold=$threshold, dataSet=$dataSet, " +
      s"dataSize=$dataSize, verticesDataPath=$verticesDataPath, edgesDataPath=$edgesDataPath, " +
      s"epsilon=$epsilon, minPts=$minPts, initialEdgeWeights=${initialEdgeWeights.mkString("[", ", ", "]")}, " +
      s"samplingThreshold=$samplingThreshold, samplingRate=$samplingRate, approach=$approach}")
  }
}
