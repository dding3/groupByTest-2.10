package org.apache.spark

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
/**
  * Created by ding on 9/27/16.
  */
object Util {
  val dropModulePercent = System.getProperty("dl.dropModulePercent", 0.05.toString).toDouble
  require(dropModulePercent<=1.0 && dropModulePercent>0)

  val moduleTimeList = new ArrayBuffer[Double]()

  def kthLargest(arr: Array[Double], l: Int, r: Int, k: Int): Double = {
    val pos = randomPartition(arr, l, r)
    if (pos-l == k-1)  return arr(pos)

    if (pos-l > k-1) return kthLargest(arr, l, pos-1, k)

    kthLargest(arr, pos+1, r, k-pos+l-1)
  }

  def swap(arr: Array[Double], i: Int, j: Int) = {
    val temp = arr(i)
    arr(i) = arr(j)
    arr(j) = temp
  }

  private def partition(arr:Array[Double], l: Int, r: Int): Int = {
    val x = arr(r)
    var i = l
    for (j <- l to (r - 1)) {
      if (arr(j) > x) {
        swap(arr, i, j);
        i += 1
      }
    }
    swap(arr, i, r);
    i
  }

  private def randomPartition(arr: Array[Double], l: Int, r: Int): Int = {
    val n = r-l+1;
    val pivot = ((Math.random()) % n).toInt;
    swap(arr, l + pivot, r);
    partition(arr, l, r);
  }
}

object parseComputeTime {
  def sqr(x: Double) = x * x

  def remove(num: Double, list: Array[Double]) = list diff Array(num)

  def remove(num1: Double, num2: Double, list: Array[Double]) = list diff Array(num1, num2)

  def indexOfLargest(array: Seq[Double]): Int = {
    val result = array.foldLeft(-1, Int.MinValue.toDouble, 0) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if (currentValue > maxValue) (currentIndex, currentValue, currentIndex + 1)
        else (maxIndex, maxValue, currentIndex + 1)
    }
    result._1
  }

  def indexOfMinum(array: Seq[Double]): Int = {
    val result = array.foldLeft(-1, Int.MaxValue.toDouble, 0) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if (currentValue < maxValue) (currentIndex, currentValue, currentIndex + 1)
        else (maxIndex, maxValue, currentIndex + 1)
    }
    result._1
  }

  def main(args: Array[String]) = {
    var sum = 0.0
//    val iteration = 2116
val iteration = 1000
    val data = new ArrayBuffer[Double]()
    for(line <- Source.fromFile(args(0)).getLines) {
      if (line.contains("task time")) {
        val time = line.split(" ").apply(3).toDouble / 1e9
        data.append(time)
//        sum += time
      }
    }
    
//    println("average: " + sum/(iteration*16*28))
//    sum = 0
    require(data.length == 2116 * 16 * 28)
    var max = 0.0
    var top5 = 0.0
    val tmp = new ArrayBuffer[Double]()
    for(i <- 0 until iteration) {
      tmp ++= data.slice(i*28, i*28+28)
      tmp ++= data.slice(i*28 + iteration*28, i*28 + iteration*28+28)
      tmp ++= data.slice(i*28 + iteration*28*2, i*28 + iteration*28*2 + 28)
      tmp ++= data.slice(i*28 + iteration*28*3, i*28 + iteration*28*3+28)
      tmp ++= data.slice(i*28 + iteration*28*4, i*28 + iteration*28*4+28)
      tmp ++= data.slice(i*28 + iteration*28*5, i*28 + iteration*28*5+28)
      tmp ++= data.slice(i*28 + iteration*28*6, i*28 + iteration*28*6+28)
      tmp ++= data.slice(i*28 + iteration*28*7, i*28 + iteration*28*7+28)
      tmp ++= data.slice(i*28 + iteration*28*8, i*28 + iteration*28*8+28)
      tmp ++= data.slice(i*28 + iteration*28*9, i*28 + iteration*28*9+28)
      tmp ++= data.slice(i*28 + iteration*28*10, i*28 + iteration*28*10+28)
      tmp ++= data.slice(i*28 + iteration*28*11, i*28 + iteration*28*11+28)
      tmp ++= data.slice(i*28 + iteration*28*12, i*28 + iteration*28*12+28)
      tmp ++= data.slice(i*28 + iteration*28*13, i*28 + iteration*28*13+28)
      tmp ++= data.slice(i*28 + iteration*28*14, i*28 + iteration*28*14+28)
      tmp ++= data.slice(i*28 + iteration*28*15, i*28 + iteration*28*15+28)
      println("last: " + (i*28 + iteration*28*15+28))
      require(tmp.length == 16*28)
      val tmpArray = tmp.toArray
      scala.util.Sorting.quickSort(tmpArray)
      max += tmpArray.last
      sum += tmpArray.sum
      val index = (16*28*0.95).toInt
      top5 += tmpArray(index)
      tmp.clear()
    }

    println("average: " + sum/(iteration*16*28))
    println("longest: " + max/iteration)
    println("top5: " + top5/iteration)
  } 
}
