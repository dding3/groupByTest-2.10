/*
 * Copyright 2016 The BigDL Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.analytics.bigdl.torch

import java.io.PrintWriter

import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl.optim.SGD
import com.intel.analytics.bigdl.tensor.{Storage, Tensor}
import com.intel.analytics.bigdl.utils.RandomGenerator._
import com.intel.analytics.bigdl.utils.TorchObject.{TYPE_DOUBLE_TENSOR, TYPE_FLOAT_TENSOR}
import com.intel.analytics.bigdl.utils.{File, T}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.sys.process._
import scala.util.Random

@com.intel.analytics.bigdl.tags.Parallel
class ConvLSTMPeepholeSpec  extends FlatSpec with BeforeAndAfter with Matchers {
  System.setProperty("bigdl.disableCheckSysEnv", "True")

  "A ConvLSTMPeepwhole " should "generate corrent output" in {
    val hiddenSize = 5
    val inputSize = 3
    val seqLength = 4
    val seed = 100
    val batchSize = 1

    RNG.setSeed(seed)

    val inputData = new Array[Double](batchSize * seqLength * inputSize * 3 * 3)
    for(i <- 0 until inputData.length) {
      inputData(i) = Random.nextDouble()
    }
    val input = Tensor[Double](inputData, Array(batchSize, seqLength, inputSize, 3, 3))
//    val input = File.loadTorch("/tmp/input-1-4-3-7-7.t7").asInstanceOf[Tensor[Double]]
    println("input: " + input)

    val rec = Recurrent[Double](hiddenSize)

    val model = Sequential[Double]()
      .add(rec
        .add(ConvLSTMPeephole[Double](inputSize, hiddenSize, 3, 3, 1)))
    val (weights, gradidents) = model.getParameters()
//    weights.copy(File.loadTorch("/tmp/weight-torch-2-4-3-7-7.t7").asInstanceOf[Tensor[Double]])
    println("weight: " + weights)

    val suffix = ".t7"
    var tmp = java.io.File.createTempFile("inputTmp", suffix)
    var tmpPath = tmp.getAbsolutePath
        File.saveTorch(input, tmpPath, TYPE_DOUBLE_TENSOR, true)
        println("input path: " + tmpPath)

    tmp = java.io.File.createTempFile("inputTorchTmp", suffix)
    tmpPath = tmp.getAbsolutePath
    File.saveTorch(input.transpose(1,2), tmpPath, TYPE_DOUBLE_TENSOR, true)
    println("torch input path: " + tmpPath)

       tmp = java.io.File.createTempFile("weightTmp", suffix)
      tmpPath = tmp.getAbsolutePath
        File.saveTorch(weights, tmpPath, TYPE_DOUBLE_TENSOR, true)
        println("weight path: " + tmpPath)
    println("weight size:" + model.getParameters()._1)

    val output = model.forward(input)
    val gradInput = model.backward(input, output)
    println("forward output: " + output.asInstanceOf[Tensor[Double]].transpose(1, 2))
    println("backward output: " + gradInput.toTensor[Double].transpose(1, 2))

    //    val tt = File.loadTorch("/tmp/conv-lstm-output.t7").asInstanceOf[com.intel.analytics.bigdl.utils.Table]
    //    println("torch output: " + tt)

    //        val tt2 = File.loadTorch("/tmp/conv-lstm-gradInput.t7").asInstanceOf[Tensor[Double]]
    //        println("torch gradInput: " + tt2)

  }

  "A ConvLSTMPeepwhole " should "generate corrent output when batch != 1" in {
    val hiddenSize = 5
    val inputSize = 2
    val seqLength = 2
    val seed = 100
    val batchSize = 3

    RNG.setSeed(seed)

    val inputData = new Array[Double](batchSize * seqLength * inputSize * 3 * 4)
    for(i <- 0 until inputData.length) {
      inputData(i) = Random.nextDouble()
    }
    val input = Tensor[Double](inputData, Array(batchSize, seqLength, inputSize, 3, 4))
    //    val input = File.loadTorch("/tmp/input-1-4-3-7-7.t7").asInstanceOf[Tensor[Double]]
    println("input: " + input)

    val rec = Recurrent[Double](hiddenSize)

    val model = Sequential[Double]()
      .add(rec
        .add(ConvLSTMPeephole[Double](inputSize, hiddenSize, 3, 3, 1)))
    val (weights, gradidents) = model.getParameters()
    //    weights.copy(File.loadTorch("/tmp/weight-torch-2-4-3-7-7.t7").asInstanceOf[Tensor[Double]])
    println("weight: " + weights)

    val suffix = ".t7"
    var tmp = java.io.File.createTempFile("inputTmp", suffix)
    var tmpPath = tmp.getAbsolutePath
    File.saveTorch(input, tmpPath, TYPE_DOUBLE_TENSOR, true)
    println("input path: " + tmpPath)

    tmp = java.io.File.createTempFile("inputTorchTmp", suffix)
    tmpPath = tmp.getAbsolutePath
    File.saveTorch(input.transpose(1,2), tmpPath, TYPE_DOUBLE_TENSOR, true)
    println("torch input path: " + tmpPath)

    tmp = java.io.File.createTempFile("weightTmp", suffix)
    tmpPath = tmp.getAbsolutePath
    File.saveTorch(weights, tmpPath, TYPE_DOUBLE_TENSOR, true)
    println("weight path: " + tmpPath)
    
    val output = model.forward(input)
    val gradInput = model.backward(input, output)
    println("forward output: " + output.asInstanceOf[Tensor[Double]].transpose(1, 2))
    println("backward output: " + gradInput.toTensor[Double].transpose(1, 2))

    //    val tt = File.loadTorch("/tmp/conv-lstm-output.t7").asInstanceOf[com.intel.analytics.bigdl.utils.Table]
    //    println("torch output: " + tt)

    //        val tt2 = File.loadTorch("/tmp/conv-lstm-gradInput.t7").asInstanceOf[Tensor[Double]]
    //        println("torch gradInput: " + tt2)

  }
  
  "A ConvLSTMPeepwhole " should "verify" in {

        val tt = File.loadTorch("/tmp/conv-lstm-output.t7").asInstanceOf[com.intel.analytics.bigdl.utils.Table]
        println("torch output: " + tt)

            val tt2 = File.loadTorch("/tmp/conv-lstm-gradInput.t7").asInstanceOf[Tensor[Double]]
            println("torch gradInput: " + tt2)

  }
}
