package org.apache.spark

import scala.io.Source
/**
  * Created by ding on 9/27/16.
  */
object parseSparkDL8 {
  def sqr(x: Double) = x * x

  def indexOfLargest(array: Seq[Double]): Int = {
    val result = array.foldLeft(-1,Int.MinValue.toDouble,0) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if(currentValue > maxValue) (currentIndex,currentValue,currentIndex+1)
        else (maxIndex,maxValue,currentIndex+1)
    }
    result._1
  }

  def indexOfMinum(array: Seq[Double]): Int = {
    val result = array.foldLeft(-1,Int.MaxValue.toDouble,0) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if(currentValue < maxValue) (currentIndex,currentValue,currentIndex+1)
        else (maxIndex,maxValue,currentIndex+1)
    }
    result._1
  }

  def main(args: Array[String]) = {

    val iterations = 10800

    var sum = 0.0
    val datas = new Array[Array[Double]](iterations)
    var i = 0
    for(line <- Source.fromFile(args(0)).getLines) {
      datas(i) = line.split(" ").drop(6).map(_.toDouble)
      require(datas(i).length==8)
      i += 1
    }


    i = 0    
    var count1 = 0
    var count2 = 0
    var count3 = 0
    var count4 = 0
    var count5 = 0
    var count6 = 0
    var count7 = 0  
    var count = 0


    var count16 = 0
    var count17 = 0
    var count18 = 0
    var count19 = 0
    var count20 = 0
    var count21 = 0
    var count22 = 0
    var count23 = 0    
    while(i < iterations) {
      val index = indexOfLargest(datas(i))
      if(index==0) count+=1
      else if (index==1) count1+=1
      else if (index==2) count2+=1
      else if (index==3) count3+=1
      else if (index==4) count4+=1
      else if (index==5) count5+=1
      else if (index==6) count6+=1
      else if (index==7) count7+=1
      
      val min = indexOfMinum(datas(i))
      if(min==0) count16+=1
      else if (min==1) count17+=1
      else if (min==2) count18+=1
      else if (min==3) count19+=1
      else if (min==4) count20+=1
      else if (min==5) count21+=1
      else if (min==6) count22+=1
      else if (min==7) count23+=1
      i += 1
    }
    
    println("47: " + count)
    println("48: " + count1)
    println("49: " + count2)
    println("50: " + count3)
    println("55: " + count4)
    println("52: " + count5)
    println("53: " + count6)
    println("54: " + count7)
 
    println("Min")
    println("47: " + count16)
    println("48: " + count17)
    println("49: " + count18)
    println("50: " + count19)
    println("55: " + count20)
    println("52: " + count21)
    println("53: " + count22)
    println("54: " + count23) 
  }
}
