//package org.apache.spark.examples.graphx
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, _}
//
//object PregelBug {
//  def main(args: Array[String]) = {
//    //FIXME breaks if TestVertex is a case class; works if not case class
//    class TestVertex(inId: VertexId,
//                          inData: String,
//                          inLabels: collection.mutable.HashSet[String]) extends Serializable {
//      val id = inId
//      val value = inData
//      val labels = inLabels
//    }
//
//    class TestVertex2(inId: VertexId,
//                          inData: String,
//                          inLabels: collection.mutable.HashSet[String]) extends Serializable {
//      val id = inId
//      val value = inData
//      val labels = inLabels
//    }
//
//    class TestLink(inSrc: VertexId, inDst: VertexId, inData: String) extends Serializable  {
//      val src = inSrc
//      val dst = inDst
//      val data = inData
//    }
//
//    val vertexesTest = Vector(
//      new TestVertex2(0, "label0", collection.mutable.HashSet[String]()),
//      new TestVertex2(1, "label1", collection.mutable.HashSet[String]())
//    )
//
//    val vertexesTest2 = Vector(
//      new TestVertex2(0, "label0", collection.mutable.HashSet[String]()),
//      new TestVertex2(1, "label1", collection.mutable.HashSet[String]())
//    )
//
//    val vertexesTest3 = Vector(
//      new TestVertex(0, "label0", collection.mutable.HashSet[String]()),
//      new TestVertex(1, "label1", collection.mutable.HashSet[String]())
//    )
//
//    val vertexesTest4 = Vector(
//      new TestVertex(0, "label0", collection.mutable.HashSet[String]()),
//      new TestVertex(1, "label1", collection.mutable.HashSet[String]())
//    )
//    
//    val startString = "XXXSTARTXXX"
//
//    val conf = new SparkConf().setAppName("pregeltest")
//    val sc = new SparkContext(conf)
//    sc.setCheckpointDir("/tmp/checkpoint")
//    val vertexes = Vector(
//      new TestVertex(0, "label0", collection.mutable.HashSet[String]()),
//      new TestVertex(1, "label1", collection.mutable.HashSet[String]())
//    )
//    val links = Vector(
//      new TestLink(0, 1, "linkData01")
//    )
//    val vertexes_packaged = vertexes.map(v => (v.id, v))
//    val links_packaged = links.map(e => Edge(e.src, e.dst, e))
//
//    val graph = Graph[TestVertex, TestLink](sc.parallelize(vertexes_packaged), sc.parallelize(links_packaged))
//
//    def vertexProgram (vertexId: VertexId, vdata: TestVertex, message: Vector[String]): TestVertex = {
//      message.foreach {
//        case `startString` =>
//          if (vdata.id == 0L)
//            vdata.labels.add(vdata.value)
//
//        case m =>
//          if (!vdata.labels.contains(m))
//            vdata.labels.add(m)
//      }
//      new TestVertex(vdata.id, vdata.value, vdata.labels)
//    }
//
//    def sendMessage (triplet: EdgeTriplet[TestVertex, TestLink]): Iterator[(VertexId, Vector[String])] = {
//      val srcLabels = triplet.srcAttr.labels
//      val dstLabels = triplet.dstAttr.labels
//
//      val msgsSrcDst = srcLabels.diff(dstLabels)
//        .map(label => (triplet.dstAttr.id, Vector[String](label)))
//
//      val msgsDstSrc = dstLabels.diff(dstLabels)
//        .map(label => (triplet.srcAttr.id, Vector[String](label)))
//
//      msgsSrcDst.toIterator ++ msgsDstSrc.toIterator
//    }
//
//    def mergeMessage (m1: Vector[String], m2: Vector[String]): Vector[String] = m1.union(m2).distinct
//
//    val g = graph.pregel(Vector[String](startString))(vertexProgram, sendMessage, mergeMessage)
//
//    println("---pregel done---")
//    println("vertex info:")
//    g.vertices.foreach(
//      v => {
//        val labels = v._2.labels
//        println(
//          "vertex " + v._1 +
//            ": name = " + v._2.id +
//            ", labels = " + labels)
//      }
//    )
//  }
//}