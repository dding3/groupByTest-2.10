package org.apache.spark

import scala.io.Source
/**
  * Created by ding on 9/27/16.
  */
object parseSparkDL {
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

  def remove(num: Double, list: Array[Double]) = list diff Array(num)
  def remove(num1: Double, num2:Double, list: Array[Double]) = list diff Array(num1, num2)

  def main(args: Array[String]) = {
   val iterations = 302
    
    var sum = 0.0
    val datas = new Array[Double](iterations * 16)
    var i = 0
    for(line <- Source.fromFile(args(0)).getLines) {
      datas(i) = line.toDouble
      i += 1
    }
    
    i = 0
    var diff0_1 = 0.0
    var diff1_2 = 0.0
    var diff2_3 = 0.0
    var diff3_4 = 0.0
    var diff4_5 = 0.0
    var diff5_6 = 0.0
    var diff6_7 = 0.0
    var diff7_8 = 0.0
    var diff8_9 = 0.0
    var diff9_10 = 0.0
    var diff10 = 0.0
    var count1 = 0
    var count2 = 0
    var count3 = 0
    var count4 = 0
    var count5 = 0
    var count6 = 0
    var count7 = 0
    var count8 = 0
    var count9 = 0
    var count10 = 0
    var count11 = 0
    var count12 = 0
    var count13 = 0
    var count14 = 0
    var count15 = 0
    var count = 0


    var count16 = 0
    var count17 = 0
    var count18 = 0
    var count19 = 0
    var count20 = 0
    var count21 = 0
    var count22 = 0
    var count23 = 0
    var count24 = 0
    var count25 = 0
    var count26 = 0
    var count27 = 0
    var count28 = 0
    var count29 = 0
    var count30 = 0
    var count31 = 0
    while(i < iterations) {
      val tmp = new Array[Double](16)
      tmp(0) = datas(i)
      tmp(1) = datas(i+iterations)
      tmp(2) = datas(i+iterations*2)
      tmp(3) = datas(i+iterations*3)
      tmp(4) = datas(i+iterations*4)
      tmp(5) = datas(i+iterations*5)
      tmp(6) = datas(i+iterations*6)
      tmp(7) = datas(i+iterations*7)
      tmp(8) = datas(i+iterations*8)
      tmp(9) = datas(i+iterations*9)
      tmp(10) = datas(i+iterations*10)
      tmp(11) = datas(i+iterations*11)
      tmp(12) = datas(i+iterations*12)
      tmp(13) = datas(i+iterations*13)
      tmp(14) = datas(i+iterations*14)
      tmp(15) = datas(i+iterations*15)
      val index = indexOfLargest(tmp)     
       
      if(index==0) count+=1
      else if (index==1) count1+=1
      else if (index==2) count2+=1
      else if (index==3) count3+=1
      else if (index==4) count4+=1
      else if (index==5) count5+=1
      else if (index==6) count6+=1
      else if (index==7) count7+=1
      else if (index==8) count8+=1
      else if (index==9) count9+=1
      else if (index==10) count10+=1
      else if (index==11) count11+=1
      else if (index==12) count12+=1
      else if (index==13) count13+=1
      else if (index==14) count14+=1
      else if (index==15) count15+=1
      
      val min = indexOfMinum(tmp)
      if(min==0) count16+=1
      else if (min==1) count17+=1
      else if (min==2) count18+=1
      else if (min==3) count19+=1
      else if (min==4) count20+=1
      else if (min==5) count21+=1
      else if (min==6) count22+=1
      else if (min==7) count23+=1
      else if (min==8) count24+=1
      else if (min==9) count25+=1
      else if (min==10) count26+=1
      else if (min==11) count27+=1
      else if (min==12) count28+=1
      else if (min==13) count29+=1
      else if (min==14) count30+=1
      else if (min==15) count31+=1
             
      tmp.foreach{x => print(x+" ")}
      val diff = tmp(index)-tmp(min)
      print(" max-min: " + diff + " min: " + tmp(min) + " max: " + tmp(index))
      sum += diff
      if(diff<=0.1) diff0_1 += 1
      else if(diff>0.1 && diff<=0.2) diff1_2 += 1
      else if(diff>0.2 && diff<=0.3) diff2_3 += 1
      else if(diff>0.3 && diff<=0.4) diff3_4 += 1
      else if(diff>0.4 && diff<=0.5) diff4_5 += 1
      else if(diff>0.5 && diff<=0.6) diff5_6 += 1
      else if(diff>0.6 && diff<=0.7) diff6_7 += 1
      else if(diff>0.7 && diff<=0.8) diff7_8 += 1
      else if(diff>0.8 && diff<=0.9) diff8_9 += 1
      else if(diff>0.9 && diff<=1.0) diff9_10 += 1
      else diff10 += 1
      println()
//      scala.util.Sorting.quickSort(tmp)
//      println(tmp(7)-tmp(0))
//      var sum = 0.0
//      var variance = 0.0
//      var index = 0
//      while(index<tmp.length) {
//        sum += tmp(index)
//        index += 1
//      }
//      val avg = sum/8
//      index = 0
//      while(index<tmp.length) {
//        variance += sqr(tmp(index)-avg)
//        index += 1
//      }
//      variances(i) = variance
//      if(variance < 0.01) count1 += 1
//      else if(variance < 0.02) count2+=1
//      else if(variance < 0.03) count3 += 1
//      else if(variance < 0.04) count4+=1
//      else if(variance < 0.05) count5 += 1
//      else if(variance < 0.06) count6+=1
//      else if(variance < 0.07) count7 += 1
//      else if(variance < 0.08) count8+=1
//      else if(variance < 0.09) count9 += 1
//      else if(variance < 0.1) count10 += 1
//      else if(variance < 0.2) count11 += 1
//      else if(variance < 0.3) count12+=1
//      else if(variance < 0.4) count13 += 1
//      else if(variance < 0.5) count14 += 1
//      else if(variance < 0.6) count15 += 1
//      else if(variance < 0.7) count16 += 1
//      else if(variance < 0.8) count17 += 1
//      else count += 1
//        println("i: " + i + " avg: " + avg + " variance: " + variance + "data: " + tmp(0) + " " + tmp(1)
//          + " " + tmp(2) + " " + tmp(3) + " " + tmp(4) + " " + tmp(5) + " " + tmp(6)
//          + " " + tmp(7))  

      i += 1      
    }
//    scala.util.Sorting.quickSort(variances)
//    variances.foreach(println(_))
    println("47: " + count)
    println("48: " + count1)
    println("49: " + count2)
    println("50: " + count3)
    println("55: " + count4)
    println("52: " + count5)
    println("53: " + count6)
    println("54: " + count7)
    println("56: " + count8)
    println("65: " + count9)
    println("58: " + count10)
    println("59: " + count11)
    println("60: " + count12)
    println("62: " + count13)
    println("63: " + count14)
    println("64: " + count15)
    
    println("Min")
    println("47: " + count16)
    println("48: " + count17)
    println("49: " + count18)
    println("50: " + count19)
    println("55: " + count20)
    println("52: " + count21)
    println("53: " + count22)
    println("54: " + count23)
    println("56: " + count24)
    println("65: " + count25)
    println("58: " + count26)
    println("59: " + count27)
    println("60: " + count28)
    println("62: " + count29)
    println("63: " + count30)
    println("64: " + count31) 
    
    println("average of difference: " + sum/iterations)
    println("diff <= 0.1, " + diff0_1/iterations)
    println("0.1 < diff <= 0.2, " + diff1_2/iterations)
    println("0.2 < diff <= 0.3, " + diff2_3/iterations)
    println("0.3 < diff <= 0.4, " + diff3_4/iterations)
    println("0.4 < diff <= 0.5, " + diff4_5/iterations)
    println("0.5 < diff <= 0.6, " + diff5_6/iterations)
    println("0.6 < diff <= 0.7, " + diff6_7/iterations)
    println("0.7 < diff <= 0.8, " + diff7_8/iterations)
    println("0.8 < diff <= 0.9, " + diff8_9/iterations)
    println("0.9 < diff <= 1, " + diff9_10/iterations)
    println("1 < diff, " + diff10/iterations)    
  }
}
