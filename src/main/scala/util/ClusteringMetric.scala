package util

import org.apache.spark.graphx._
import breeze.linalg.{SparseVector => SV}
import breeze.linalg._
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

object ClusteringMetric {

  // 原始方案
  def density(edgeWeightUpdateGraph: Graph[(Long, Long), Long]): Double ={
    val ClusteredGraph = edgeWeightUpdateGraph.subgraph(vpred = (_, attr) => attr._2 > 0L)  // 只筛选出被聚类的顶点
    val totalEdgesNum = ClusteredGraph.numEdges
    val insideClusters: VertexRDD[Long] = ClusteredGraph.aggregateMessages[Long](
      triplet => {
        if(triplet.dstAttr._2 == triplet.srcAttr._2){
          triplet.sendToDst(1L)
        }
      },
      (a,b) => a + b
    )
    val insideClustersEdgeNum = insideClusters.map(x => x._2).reduce(_+_)
    val density = 1.0 * insideClustersEdgeNum / totalEdgesNum

    density
  }

  // 原始方案
  def entropy(sc: SparkContext, edgeWeightUpdateGraph: Graph[(Long, Long), Long], edgeWeights: Array[Double]):
  Double ={
    val attributeNum = edgeWeights.length - 1

    // 计算每种属性的顶点个数
    val vertexNumByTypeArray = edgeWeightUpdateGraph.vertices.groupBy(_._2._1).map(
      kv => (kv._1, kv._2.count(_ => true))
    ).sortBy(_._1).collect    // 排序保证顺序
    val vertexNumByTypeArrayBC = sc.broadcast(vertexNumByTypeArray)

    // 得到每个属性的起始编号
    val startPosArray = vertexNumByTypeArrayBC.value.map(kv => kv._2)
    for(i <- 1 until startPosArray.length){
      startPosArray.update(i, startPosArray(i-1) + startPosArray(i))
    }
    val startPosArrayBC = sc.broadcast(startPosArray)

    // 初始化频率统计稀疏向量数组
    val attributeZeros = vertexNumByTypeArrayBC.value.map(
      kv => SV.zeros[Double](kv._2.toInt)
    ).drop(1)

    val initialFrequencyGraph = edgeWeightUpdateGraph.mapVertices(
      (_, attr) => (attr._1, attr._2, attributeZeros)
    )
    //    initialFrequencyGraph.vertices.sortBy(_._1).foreach(println(_))

    // initialMsg
    val initialMessage = attributeZeros

    // vprog func
    def vertexProgram(vid: VertexId, attr: (Long, Long, Array[SV[Double]]), msgSumOpt: Array[SV[Double]]):
    (Long, Long, Array[SV[Double]]) = {
      val newValues = new ArrayBuffer[SV[Double]]
      for(i <- msgSumOpt.indices){
        newValues += attr._3(i) +:+ msgSumOpt(i)
      }

      (attr._1, attr._2, newValues.toArray)
    }

    // sendMsg func
    def sendMessage(edge: EdgeTriplet[(Long, Long, Array[SV[Double]]), Long]):
    Iterator[(VertexId, Array[SV[Double]])] = {
      val srcVertexType = edge.srcAttr._1
      val dstClusterId = edge.dstAttr._2

      if(srcVertexType != 0L && dstClusterId > 0L){

        val attributeIndex = (srcVertexType - 1).toInt
        val pos = (edge.srcId - startPosArrayBC.value(attributeIndex)).toInt

        val attributeMsg = vertexNumByTypeArrayBC.value.map(
          kv => SV.zeros[Double](kv._2.toInt)
        ).drop(1)
        attributeMsg(attributeIndex).update(pos, 1.0)  // 属性顶点计数

        Iterator((edge.dstId, attributeMsg))
      }
      else {
        Iterator.empty
      }
    }

    // mergeMsg func
    def mergeMessage(a: Array[SV[Double]], b: Array[SV[Double]]): Array[SV[Double]] = {
      val msg = new ArrayBuffer[SV[Double]]
      for(i <- a.indices){
        msg += a(i) +:+ b(i)
      }

      msg.toArray
    }

    // Execute a dynamic version of Pregel
    val FrequencyGraph = Pregel(
      graph = initialFrequencyGraph,
      initialMsg = initialMessage,
      maxIterations = 1,  // 只处理邻居顶点
      activeDirection = EdgeDirection.Out)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage
    )

    //    println(s"e ==== ${FrequencyGraph.vertices.filter(_._2._2 > 0L).count()}")

    val attributeEntropyArray = FrequencyGraph.vertices
      .filter(_._2._2 > 0L)  // 筛选出被正确聚类的主类顶点
      .map(
      kv => (kv._2._2, kv._2._3)  // (clusterId, frequencyArray)
    )
      .reduceByKey(  // 统计每个簇内的每个属性对应的顶点频率
        (x, y) => {
          val values = new ArrayBuffer[SV[Double]]
          for(i <- x.indices){
            values += x(i) +:+ y(i)
          }
          values.toArray
        }
      )
      .map(  // 计算每个簇内的每个属性对应的熵
        kv => {
          kv._2.map(
            frequency => {
              if(frequency.activeSize == 0){  // 处理不存在某些类型的属性边的情况
                0.0
              }
              else{
                frequency :/= sum(frequency)  // 计算每个属性对应的顶点概率
                frequency.activeValuesIterator.map(x => x * Math.log(x)).sum * (-1)  // 计算每个属性对应的熵
              }
            }
          )
        }
      )
      .reduce(  // 统计所有簇的属性熵
        (x, y) => x +:+ y
      )

    val entropy = attributeEntropyArray.sum / attributeNum

    entropy
  }

  // 添加 entropy / cluster size
  def entropy2(sc: SparkContext, edgeWeightUpdateGraph: Graph[(Long, Long), Long], edgeWeights: Array[Double]):
  Double ={
    val attributeNum = edgeWeights.length - 1

    // 计算每种属性的顶点个数
    val vertexNumByTypeArray = edgeWeightUpdateGraph.vertices.groupBy(_._2._1).map(
      kv => (kv._1, kv._2.count(_ => true))
    ).sortBy(_._1).collect    // 排序保证顺序
    val vertexNumByTypeArrayBC = sc.broadcast(vertexNumByTypeArray)

    // 得到每个属性的起始编号
    val startPosArray = vertexNumByTypeArrayBC.value.map(kv => kv._2)
    for(i <- 1 until startPosArray.length){
      startPosArray.update(i, startPosArray(i-1) + startPosArray(i))
    }
    val startPosArrayBC = sc.broadcast(startPosArray)

    // 初始化频率统计稀疏向量数组
    val attributeZeros = vertexNumByTypeArrayBC.value.map(
      kv => SV.zeros[Double](kv._2.toInt)
    ).drop(1)

    val initialFrequencyGraph = edgeWeightUpdateGraph.mapVertices(
      (_, attr) => (attr._1, attr._2, attributeZeros)
    )
    //    initialFrequencyGraph.vertices.sortBy(_._1).foreach(println(_))

    // initialMsg
    val initialMessage = attributeZeros

    // vprog func
    def vertexProgram(vid: VertexId, attr: (Long, Long, Array[SV[Double]]), msgSumOpt: Array[SV[Double]]):
    (Long, Long, Array[SV[Double]]) = {
      val newValues = new ArrayBuffer[SV[Double]]
      for(i <- msgSumOpt.indices){
        newValues += attr._3(i) +:+ msgSumOpt(i)
      }

      (attr._1, attr._2, newValues.toArray)
    }

    // sendMsg func
    def sendMessage(edge: EdgeTriplet[(Long, Long, Array[SV[Double]]), Long]):
    Iterator[(VertexId, Array[SV[Double]])] = {
      val srcVertexType = edge.srcAttr._1
      val dstClusterId = edge.dstAttr._2

      if(srcVertexType != 0L && dstClusterId > 0L){

        val attributeIndex = (srcVertexType - 1).toInt
        val pos = (edge.srcId - startPosArrayBC.value(attributeIndex)).toInt

        val attributeMsg = vertexNumByTypeArrayBC.value.map(
          kv => SV.zeros[Double](kv._2.toInt)
        ).drop(1)
        attributeMsg(attributeIndex).update(pos, 1.0)  // 属性顶点计数

        Iterator((edge.dstId, attributeMsg))
      }
      else {
        Iterator.empty
      }
    }

    // mergeMsg func
    def mergeMessage(a: Array[SV[Double]], b: Array[SV[Double]]): Array[SV[Double]] = {
      val msg = new ArrayBuffer[SV[Double]]
      for(i <- a.indices){
        msg += a(i) +:+ b(i)
      }

      msg.toArray
    }

    // Execute a dynamic version of Pregel
    val frequencyGraph = Pregel(
      graph = initialFrequencyGraph,
      initialMsg = initialMessage,
      maxIterations = 1,  // 只处理邻居顶点
      activeDirection = EdgeDirection.Out)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage
    )

    //    println(s"e ==== ${FrequencyGraph.vertices.filter(_._2._2 > 0L).count()}")

    // 提取被聚类的主类顶点
    val clusteredVertices = frequencyGraph.vertices
      .filter(_._2._2 > 0L)  // 筛选出被正确聚类的主类顶点
      .map(
        kv => (kv._2._2, kv._2._3)  // (clusterId, frequencyArray)
     )

    // 统计每个簇的大小
    val clusterSizeMap = clusteredVertices.countByKey()
    val clusterSizeMapBC = sc.broadcast(clusterSizeMap)

    val attributeEntropyArray = clusteredVertices
      .reduceByKey(  // 统计每个簇内的每个属性对应的顶点频率
        (x, y) => {
          val values = new ArrayBuffer[SV[Double]]
          for(i <- x.indices){
            values += x(i) +:+ y(i)
          }
          values.toArray
        }
      )
      .map(  // 计算每个簇内的每个属性对应的熵
        kv => {  // (clusterId: Long, frequencyArray: Array[SV[Double]])
          val entropyArray = kv._2.map(
            frequency => {
              if(frequency.activeSize == 0){  // 处理不存在某些类型的属性边的情况
                0.0
              }
              else{
                frequency :/= sum(frequency)  // 计算每个属性对应的顶点概率
                frequency.activeValuesIterator.map(x => x * Math.log(x)).sum * (-1) // 计算每个属性对应的熵
              }
            }
          )
          (kv._1, entropyArray)
        }
      )
      .map(
        kv => {
          // normalizedEntropyArray
          kv._2.map(x => x / clusterSizeMapBC.value(kv._1).toDouble)  // 每个簇对应的属性熵除以当前簇的大小, 进行归一化
        }
      )
      .reduce(  // 统计所有簇的属性熵
        (x, y) => x +:+ y
      )

    val entropy = attributeEntropyArray.sum / attributeNum

    entropy
  }

  /// 添加 entropy / frequency.activeSize
  def entropy3(sc: SparkContext, edgeWeightUpdateGraph: Graph[(Long, Long), Long], edgeWeights: Array[Double]):
  Double ={
    val attributeNum = edgeWeights.length - 1

    // 计算每种属性的顶点个数
    val vertexNumByTypeArray = edgeWeightUpdateGraph.vertices.groupBy(_._2._1).map(
      kv => (kv._1, kv._2.count(_ => true))
    ).sortBy(_._1).collect    // 排序保证顺序
    val vertexNumByTypeArrayBC = sc.broadcast(vertexNumByTypeArray)

    // 得到每个属性的起始编号
    val startPosArray = vertexNumByTypeArrayBC.value.map(kv => kv._2)
    for(i <- 1 until startPosArray.length){
      startPosArray.update(i, startPosArray(i-1) + startPosArray(i))
    }
    val startPosArrayBC = sc.broadcast(startPosArray)

    // 初始化频率统计稀疏向量数组
    val attributeZeros = vertexNumByTypeArrayBC.value.map(
      kv => SV.zeros[Double](kv._2.toInt)
    ).drop(1)

    val initialFrequencyGraph = edgeWeightUpdateGraph.mapVertices(
      (_, attr) => (attr._1, attr._2, attributeZeros)
    )
    //    initialFrequencyGraph.vertices.sortBy(_._1).foreach(println(_))

    // initialMsg
    val initialMessage = attributeZeros

    // vprog func
    def vertexProgram(vid: VertexId, attr: (Long, Long, Array[SV[Double]]), msgSumOpt: Array[SV[Double]]):
    (Long, Long, Array[SV[Double]]) = {
      val newValues = new ArrayBuffer[SV[Double]]
      for(i <- msgSumOpt.indices){
        newValues += attr._3(i) +:+ msgSumOpt(i)
      }

      (attr._1, attr._2, newValues.toArray)
    }

    // sendMsg func
    def sendMessage(edge: EdgeTriplet[(Long, Long, Array[SV[Double]]), Long]):
    Iterator[(VertexId, Array[SV[Double]])] = {
      val srcVertexType = edge.srcAttr._1
      val dstClusterId = edge.dstAttr._2

      if(srcVertexType != 0L && dstClusterId > 0L){

        val attributeIndex = (srcVertexType - 1).toInt
        val pos = (edge.srcId - startPosArrayBC.value(attributeIndex)).toInt

        val attributeMsg = vertexNumByTypeArrayBC.value.map(
          kv => SV.zeros[Double](kv._2.toInt)
        ).drop(1)
        attributeMsg(attributeIndex).update(pos, 1.0)  // 属性顶点计数

        Iterator((edge.dstId, attributeMsg))
      }
      else {
        Iterator.empty
      }
    }

    // mergeMsg func
    def mergeMessage(a: Array[SV[Double]], b: Array[SV[Double]]): Array[SV[Double]] = {
      val msg = new ArrayBuffer[SV[Double]]
      for(i <- a.indices){
        msg += a(i) +:+ b(i)
      }

      msg.toArray
    }

    // Execute a dynamic version of Pregel
    val FrequencyGraph = Pregel(
      graph = initialFrequencyGraph,
      initialMsg = initialMessage,
      maxIterations = 1,  // 只处理邻居顶点
      activeDirection = EdgeDirection.Out)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage
    )

    //    println(s"e ==== ${FrequencyGraph.vertices.filter(_._2._2 > 0L).count()}")

    val attributeEntropyArray = FrequencyGraph.vertices
      .filter(_._2._2 > 0L)  // 筛选出被正确聚类的主类顶点
      .map(
      kv => (kv._2._2, kv._2._3)  // (clusterId, frequencyArray)
    )
      .reduceByKey(  // 统计每个簇内的每个属性对应的顶点频率
        (x, y) => {
          val values = new ArrayBuffer[SV[Double]]
          for(i <- x.indices){
            values += x(i) +:+ y(i)
          }
          values.toArray
        }
      )
      .map(  // 计算每个簇内的每个属性对应的熵
        kv => {
          kv._2.map(
            frequency => {
              if(frequency.activeSize == 0){  // 处理不存在某些类型的属性边的情况
                0.0
              }
              else{
                frequency :/= sum(frequency)  // 计算每个属性对应的顶点概率
                frequency.activeValuesIterator
                  .map(x => x * Math.log(x)).sum * (-1) / frequency.activeSize // 计算每个属性对应的熵
              }
            }
          )
        }
      )
      .reduce(  // 统计所有簇的属性熵
        (x, y) => x +:+ y
      )

    val entropy = attributeEntropyArray.sum / attributeNum

    entropy
  }

}
