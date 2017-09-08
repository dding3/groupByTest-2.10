package org.apache.spark

import java.io.File
import java.net.InetAddress
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Calendar
import java.util.concurrent.{Callable, Executors, TimeUnit}

import collection.JavaConversions._
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId, TestBlockId}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

/**
  * Created by ding on 9/23/16.
  */
object AppTest {
  def indexOfLargest(array: Seq[Double]): Int = {
    val result = array.foldLeft(-1,Double.MinValue.toDouble,0) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if(currentValue > maxValue) (currentIndex,currentValue,currentIndex+1)
        else (maxIndex,maxValue,currentIndex+1)
    }
    result._1
  }  

  def main(args: Array[String]): Unit = {
    var sum = 0.0
    var number = 0
    for (line <- Source.fromFile(args(0)).getLines) {
//      if (line.contains("computing time for each node")) {
      if (line.contains("get weights for each node")) {
        val tmp = line.split(" ").drop(6).map(_.toDouble)
        require(tmp.length == 78)
        println(tmp(indexOfLargest(tmp)))
        sum += tmp(indexOfLargest(tmp))
        number += 1
      }
    }
    println("number: " + number)
    println("average: " + sum/number)
  }

//  def main(args: Array[String]): Unit = {
//    var sum = 0.0
//    var number = 0
//    var sum2 = 0.0
//    var number2 = 0
//    val hashmap = mutable.HashMap[String, Int]()
//    for (line <- Source.fromFile(args(0)).getLines) {
//      if (line.contains("78/78")) {
//        val tmp = line.split(" ")
//        if (tmp(15).toDouble > 2000) {
//          sum += tmp(15).toDouble
//          if (hashmap.contains(tmp(18))) {
//            hashmap(tmp(18)) += 1
//          } else {
//            hashmap(tmp(18)) = 1
//          }
//          number += 1  
//        } else {
//          sum2 += tmp(15).toDouble
//          number2 += 1
//        }
//      }
//    }
//    println("number: " + number)
//    println("average: " + sum/number)
//    println("number2: " + number2)
//    println("average2: " + sum2/number2)
//    hashmap.foreach(p => println(s"key ${p._1} value ${p._2}"))
//  }
}
