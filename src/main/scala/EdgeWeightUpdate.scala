import org.apache.spark.graphx._
import breeze.linalg.{SparseVector => SV}
import breeze.linalg._
import breeze.numerics.log
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

object EdgeWeightUpdate {

  def updateEdgeWeight(
    sc: SparkContext,
    edgeWeightUpdateGraph: Graph[(Long, Long), Long],  // [(vertexTypeId, clusterId), edgeTypeId]
    oldEdgeWeights: Array[Double]): Array[Double] = {

    // 计算每种属性的顶点个数
    val vertexNumByTypeArray = edgeWeightUpdateGraph.vertices.groupBy(_._2._1).map(
      kv => (kv._1, kv._2.count(_ => true))
    ).sortBy(_._1).collect    // 排序保证顺序
    val vertexNumByTypeArrayBC = sc.broadcast(vertexNumByTypeArray)
//    vertexNumByTypeArrayBC.value.foreach(println(_))
//    println("==")

    // 得到每个属性的起始编号
    val startPosArray = vertexNumByTypeArrayBC.value.map(kv => kv._2)
    for(i <- 1 until startPosArray.length){
      startPosArray.update(i, startPosArray(i-1) + startPosArray(i))
    }
    val startPosArrayBC = sc.broadcast(startPosArray)
//    startPosArrayBC.value.foreach(println(_))

    // 初始化频率统计稀疏向量数组
    val attributeZeros = vertexNumByTypeArrayBC.value.map(
      kv => SV.zeros[Double](kv._2.toInt)
    ).drop(1)

    val initialFrequencyGraph = edgeWeightUpdateGraph.mapVertices(
      (_, attr) => (attr._1, attr._2, attributeZeros)
    )

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

//        println(s"vid = ${edge.srcId}, attributeIndex = $attributeIndex, pos = $pos")

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
    initialFrequencyGraph.unpersist()

//    println(s"e ==== ${frequencyGraph.vertices.filter(_._2._2 > 0L).count()}")

    val attributeEntropyArray = frequencyGraph.vertices
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
//          println("================")
          val t = kv._2.map(
            frequency => {
//              println(frequency)
              if(frequency.activeSize == 0){  // 处理不存在某些类型的属性边的情况
                0.0
              }
              else{
                frequency :/= sum(frequency)  // 计算每个属性对应的顶点概率
                frequency.activeValuesIterator.map(x => x * Math.log(x)).sum * (-1)  // 计算每个属性对应的熵
              }
            }
          )
//          print(s"${kv._1}: \t")
//          t.foreach(x => print(s"$x\t"))
//          println()
          t
        }
      )
//      .foreach(
//        x => {
//          x.foreach( y => print(s"$y\t"))
//          println()
//        }
//      )
      .reduce(  // 统计所有簇的属性熵
        (x, y) => x +:+ y
      )
//        .foreach(
//          x => {
//            print(s"$x\t")
//          }
//        )
//      println()
    frequencyGraph.unpersist()

//    attributeEntropyArray.foreach(println(_))

    val attributeInfluenceArray = attributeEntropyArray.map(
      x => {
        // 处理熵为0的情况
        val modifiedEntropy = if(x < 1E-6) attributeEntropyArray.sum / attributeEntropyArray.length else x
        // influence
        1.0 / modifiedEntropy
      }
    )

    val attributeWeightSum = oldEdgeWeights.sum - 1.0
    val deltaAttributeWeights = attributeInfluenceArray.map(
      x => {
        (x / attributeInfluenceArray.sum) * attributeWeightSum
      }
    )

    val newEdgeWeights = new ArrayBuffer[Double]
    newEdgeWeights += 1.0
    for(i <- 1 until oldEdgeWeights.length){
      newEdgeWeights += (oldEdgeWeights(i) + deltaAttributeWeights(i-1)) / 2.0
    }

    newEdgeWeights.toArray
  }

  def updateEdgeWeight2(
    sc: SparkContext,
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
      if(Math.abs(entropyai(i)) <  1e-6){
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
