package org.apache.spark

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
/**
  * Created by ding on 9/27/16.
  */
object parseSparkDL16_task {
  def sqr(x: Double) = x * x

  def remove(num: Double, list: Array[Double]) = list diff Array(num)
  def remove(num1: Double, num2:Double, list: Array[Double]) = list diff Array(num1, num2)

  def indexOfLargest(array: Seq[Long]): Int = {
    val result = array.foldLeft(-1,Long.MinValue.toDouble,0) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if(currentValue > maxValue) (currentIndex,currentValue,currentIndex+1)
        else (maxIndex,maxValue,currentIndex+1)
    }
    result._1
  }

  def indexOfMinum(array: Seq[Double]): Int = {
    val result = array.foldLeft(-1,Long.MaxValue.toDouble,0) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if(currentValue < maxValue) (currentIndex,currentValue,currentIndex+1)
        else (maxIndex,maxValue,currentIndex+1)
    }
    result._1
  }

  private def truncate(value: Float): Int = {
    java.lang.Float.floatToRawIntBits(value) & 0xffff0000
  }

  private def toFloat(byte1: Byte, byte2: Byte): Float = {
    java.lang.Float.intBitsToFloat(byte1 << 24 | byte2 << 16 & 0x00ff0000)
  }
  
  def main(args: Array[String]) = {
    var task1 = 0.0
    var task2 = 0.0
    var task3 = 0.0

    var task1Sum = 0.0
    var task2Sum = 0.0
    var task3Sum = 0.0
    var num = 0
    var num1 = 0
    var num2 = 0
        
//    var isTask2 = true
//    val lists = ArrayBuffer[Int]()
//    for (i <- 0 until 20) {
//      lists.append(23 + 10 * i)
//    }
//      for (i <- 0 until 20) {
//        lists.append(192 + 171 * i)
//      }
//    lists.append(226)
//    for(line <- Source.fromFile(args(0)).getLines ) {
//        if (line.contains("(1/448)") ) {
//          val tmp = line.split(" ")
//          if (!lists.contains(tmp(11).toDouble)) {
//            task1 = tmp(15).toDouble
//            task1Sum += task1
////            println(task1)
//            num += 1
//          }
//        } else if (line.contains("(1/16)")) {
//          val tmp = line.split(" ")
//          if (isTask2) {
//            task2 = tmp(15).toDouble
//            task2Sum += (task2 - task1)
//            println(task2-task1)
////            task2Sum += task2
//            isTask2 = false
//            num1 += 1
//          } else {
//            num2 += 1
//            task3 = tmp(15).toDouble
//            task3Sum += (task3 - task2)
////            task3Sum += task3
//            isTask2 = true
//          }
//        } 
//      }
    
//    var isTask2 = false
//    var isTask1 = true
//    val lists = ArrayBuffer[Int]()
////    for (i <- 0 until 20) {
////      lists.append(192 + 171 * i)
////    }
//    lists.append(4306)
//    var sum = 0.0
//    for(line <- Source.fromFile(args(0)).getLines ) {
//      if (line.contains("(16/16)")) {
//        val tmp = line.split(" ")
////        if (tmp(11).toDouble<3441 && !lists.contains(tmp(11).toDouble)) {
//        if (!lists.contains(tmp(11).toDouble)) {
//          if (isTask1) {
//            task1 = tmp(15).toDouble
//            task1Sum += task1
////            println(task1 + " stage: " +tmp(11) )
//            println(task1)
//            isTask2 = true
//            isTask1 = false
//            sum += task1
//            num += 1
//          } else if (isTask2) {
//            task2 = tmp(15).toDouble
//            task2Sum += task2
//            isTask2 = false
//            sum += task2
//            num1 += 1
//          } else {
//            task3 = tmp(15).toDouble
//            task3Sum += task3
//            isTask1 = true
//            sum += task3
//            num2 += 1
//          } 
//        }
//      }
//    }
    
    println("iteartion: " + num)
    println("iteartion1: " + num1)
    println("iteartion2: " + num2)
    println("task1 average: " + task1Sum/num)
    println("task2 average: " + task2Sum/num)
    println("task3 average: " + task3Sum/num)
  }
}
