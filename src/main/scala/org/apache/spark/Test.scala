package org.apache.spark

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by ding on 11/1/16.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"Test")    
    val sc = new SparkContext(conf)
    
    val data = Array(1, 2, 3, 4, 5, 6, 7)
    
    val rdd = sc.parallelize(data, 1)
    var i = 0
    while(i < 7) {
      rdd.mapPartitions { iter =>
        println(iter.next())
        Iterator.empty
      }.count()

      i += 1
    }
  }
}

class Test() extends Serializable{
  val t = 0
  val t2 = 1
  
  def tttt() = {
    val m = 0
  }
}
