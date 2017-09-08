package org.apache.spark

import java.nio.ByteBuffer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by ding on 9/23/16.
  */
object AppTest2 {
  def main(args: Array[String]): Unit = {    
    val conf = new SparkConf().setAppName(s"BMTest")
    val size = args(0).toInt
    val sc = new SparkContext(conf)

//    val data = sc.parallelize(1 to 100, args(2).toInt)    
//    var i = 0
//    while(i < args(1).toInt) {      
//      val test = data.mapPartitions { iter =>
//        val N = size        
//        val test = new Array[Byte](N)
//        Random.nextBytes(test)        
//        Iterator(test)
//      }.cache()
//      test.count()
//      test.collect()
//      test.unpersist()
//      i += 1
//    }
        
    val partitionNum = args(1).toInt
    var i = 0
    while(i < 91) {
      val data = sc.parallelize(0 until 20, partitionNum).flatMap { i =>        
        val random = new java.util.Random(i)
        Iterator.fill(size * partitionNum / 20)((random.nextInt() + i, i))
      }.cache()
      data.count()
      val partitioner = new RangePartitioner(partitionNum, data)
      val res2 = new ShuffledRDD[Int, Int, Int](data, partitioner)
      res2.count()
      i += 1
    }
  }
}
