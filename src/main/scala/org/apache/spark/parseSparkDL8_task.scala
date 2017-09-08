package org.apache.spark

import scala.io.Source
/**
  * Created by ding on 9/27/16.
  */
object parseSparkDL8_task {
  def sqr(x: Double) = x * x

  def remove(num: Double, list: Array[Double]) = list diff Array(num)
  def remove(num1: Double, num2:Double, list: Array[Double]) = list diff Array(num1, num2)
  
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

    val iterations = 1628

    var sumDiff = 0.0
    var maxSum = 0.0
    var sum = 0.0
    val datas = new Array[Double](iterations * 2 * 8)    
    var i = 0
    for(line <- Source.fromFile(args(0)).getLines) {
      datas(i) = line.toDouble/1e3
      i += 1
    }
    
    val data1 = new Array[Double](iterations * 8)
    val data2 = new Array[Double](iterations * 8)
    
    i=0
    while (i < datas.length/2) {
      data1(i) = datas(i*2)
      data2(i) = datas(i*2 + 1)
      i += 1
    }
val data = data2
val scale = 10
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
      var tmp = new Array[Double](8)
      tmp(0) = data(i)
      tmp(1) = data(i+iterations)
      tmp(2) = data(i+iterations*2)
      tmp(3) = data(i+iterations*3)
      tmp(4) = data(i+iterations*4)
      tmp(5) = data(i+iterations*5)
      tmp(6) = data(i+iterations*6)
      tmp(7) = data(i+iterations*7)

      tmp.foreach{x => 
        print(x+" ")
        sum += x
      }

      val lastIndex = indexOfLargest(tmp)
            maxSum += tmp(lastIndex)
//      val tmp_remove1 = remove(tmp(lastIndex), tmp)
//      val secondLastIndex = indexOfLargest(tmp_remove1)
//      //      maxSum += tmp_remove1(secondLastIndex)
//      val tmp_remove2 = remove(tmp_remove1(secondLastIndex), tmp_remove1)
//      val thirdLastIndex = indexOfLargest(tmp_remove2)
//      //      maxSum += tmp_remove2(thirdLastIndex)
//      val tmp_remove3 = remove(tmp_remove2(thirdLastIndex), tmp_remove2)
//      val forthLastIndex = indexOfLargest(tmp_remove3)
//      //      maxSum += tmp_remove3(forthLastIndex)
//
//      val tmp_remove4 = remove(tmp_remove3(forthLastIndex), tmp_remove3)
//      val fifthLastIndex = indexOfLargest(tmp_remove4)
//      //      maxSum += tmp_remove4(fifthLastIndex)
//      val tmp_remove5 = remove(tmp_remove4(fifthLastIndex), tmp_remove4)
//      val sixthLastIndex = indexOfLargest(tmp_remove5)
//      //      maxSum += tmp_remove5(sixthLastIndex)
//
//      val tmp_remove6 = remove(tmp_remove5(sixthLastIndex), tmp_remove5)
//      val seventhLastIndex = indexOfLargest(tmp_remove6)
//      //      maxSum += tmp_remove6(seventhLastIndex)
//
//      val tmp_remove7 = remove(tmp_remove6(seventhLastIndex), tmp_remove6)
//      val eightthLastIndex = indexOfLargest(tmp_remove7)
//      //      maxSum += tmp_remove7(eightthLastIndex)
//
//      val tmp_remove8 = remove(tmp_remove7(eightthLastIndex), tmp_remove7)
//      val ninthLastIndex = indexOfLargest(tmp_remove8)
//      maxSum += tmp_remove8(ninthLastIndex)
      //            tmp = tmp_remove3
      
      val index = lastIndex
//      if(index==0) count+=1
//      else if (index==1) count1+=1
//      else if (index==2) count2+=1
//      else if (index==3) count3+=1
//      else if (index==4) count4+=1
//      else if (index==5) count5+=1
//      else if (index==6) count6+=1
//      else if (index==7) count7+=1


      val min = indexOfMinum(tmp)
//      if(min==0) count16+=1
//      else if (min==1) count17+=1
//      else if (min==2) count18+=1
//      else if (min==3) count19+=1
//      else if (min==4) count20+=1
//      else if (min==5) count21+=1
//      else if (min==6) count22+=1
//      else if (min==7) count23+=1
      
      val diff = tmp(index)-tmp(min)
      print(" max-min: " + diff)
      sumDiff += diff
//      if(diff<=1 * scale) diff0_1 += 1
//      else if(diff>1 * scale && diff<=2 * scale) diff1_2 += 1
//      else if(diff>2 * scale && diff<=3 * scale) diff2_3 += 1
//      else if(diff>3 * scale && diff<=4 * scale) diff3_4 += 1
//      else if(diff>4 * scale && diff<=5 * scale) diff4_5 += 1
//      else if(diff>5 * scale && diff<=6 * scale) diff5_6 += 1
//      else if(diff>6 * scale && diff<=7 * scale) diff6_7 += 1
//      else if(diff>7 * scale && diff<=8 * scale) diff7_8 += 1
//      else if(diff>8 * scale && diff<=9 * scale) diff8_9 += 1
//      else if(diff>9 * scale && diff<=10 * scale) diff9_10 += 1
//      else diff10 += 1
      println()
      i += 1
    }    
//    println("47: " + count)
//    println("48: " + count1)
//    println("49: " + count2)
//    println("50: " + count3)
//    println("55: " + count4)
//    println("52: " + count5)
//    println("53: " + count6)
//    println("54: " + count7)
//
//    println("Min")
//    println("47: " + count16)
//    println("48: " + count17)
//    println("49: " + count18)
//    println("50: " + count19)
//    println("55: " + count20)
//    println("52: " + count21)
//    println("53: " + count22)
//    println("54: " + count23)

    println("average : " + sum/(iterations*8))
    println("average of difference: " + sumDiff/iterations)
//    println("diff <= 0.1, " + diff0_1/iterations)
//    println("0.1 < diff <= 0.2, " + diff1_2/iterations)
//    println("0.2 < diff <= 0.3, " + diff2_3/iterations)
//    println("0.3 < diff <= 0.4, " + diff3_4/iterations)
//    println("0.4 < diff <= 0.5, " + diff4_5/iterations)
//    println("0.5 < diff <= 0.6, " + diff5_6/iterations)
//    println("0.6 < diff <= 0.7, " + diff6_7/iterations)
//    println("0.7 < diff <= 0.8, " + diff7_8/iterations)
//    println("0.8 < diff <= 0.9, " + diff8_9/iterations)
//    println("0.9 < diff <= 1, " + diff9_10/iterations)
//    println("1 < diff, " + diff10/iterations)

    println("job1 average : " + maxSum/iterations)
    //    println("diff <= 0.1, " + diff0_1)
    //    println("0.1 < diff <= 0.2, " + diff1_2)
    //    println("0.2 < diff <= 0.3, " + diff2_3)
    //    println("0.3 < diff <= 0.4, " + diff3_4)
    //    println("0.4 < diff <= 0.5, " + diff4_5)
    //    println("0.5 < diff <= 0.6, " + diff5_6)
    //    println("0.6 < diff <= 0.7, " + diff6_7)
    //    println("0.7 < diff <= 0.8, " + diff7_8)
    //    println("0.8 < diff <= 0.9, " + diff8_9)
    //    println("0.9 < diff <= 1, " + diff9_10)
    //    println("1 < diff, " + diff10)
  }
}
