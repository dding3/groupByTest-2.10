package org.apache.spark

import java.nio.ByteBuffer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId, TestBlockId}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by ding on 9/23/16.
  */
object CoalescTest {
  def main(args: Array[String]): Unit = {    
    val conf = new SparkConf().setAppName(s"Test")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(1 to 100, 4)
    val rdd = data.mapPartitionsWithIndex { (pid, iter) =>
      val blockId = new TestBlockId(pid.toString)
      SparkEnv.get.blockManager.getRemoteBytes(blockId)
      
      Iterator.empty
    }    
    rdd.count()

    val rdd2 = data.mapPartitionsWithIndex { (pid, iter) =>
      val blockId = new TestBlockId(pid.toString)      
      
      SparkEnv.get.blockManager.getLocal(blockId)
      .map(_.data.next().asInstanceOf[Int])
      .getOrElse(throw new IllegalStateException("Please initialize AllReduceParameter first!"))
      Iterator.empty
    }.count()
  }
}
