/*
 * Licensed to Intel Corporation under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * Intel Corporation licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.analytics.bigdl.parameters

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Callable, Executors, Future, ThreadFactory}

import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.{Engine, T, Table}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.log4j.Logger
import org.apache.spark.sparkExtension.SparkExtension
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.storage.{BlockId, BlockManagerWrapper, StorageLevel}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect._

object ParameterManager {
  private val syncPoolSize: Int = System.getProperty(
    "bigdl.Parameter.syncPoolSize", "4").toInt

  val logger = Logger.getLogger(getClass)
  val syncPool = Executors.newFixedThreadPool(syncPoolSize, new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = Executors.defaultThreadFactory().newThread(r)
      t.setDaemon(true)
      t
    }
  })
}

class ParameterManager[T: ClassTag](executorId: String, executorNum: Int) {
  import ParameterManager._
  
  val gradientBuffer = new ArrayBuffer[Tensor[T]]()
  val weightBuffer = new ArrayBuffer[Tensor[T]]()
  val blockIdBuffer = new ArrayBuffer[BlockId]()
  
  def aggregateLocalGradient() : Unit = {
    require(blockIdBuffer.size == gradientBuffer.size)
    
    val length = gradientBuffer(0).nElement()
    val poolSize = Engine.default.getPoolSize
    val innerTaskSize = length / poolSize
    val innerExtraSize = length % poolSize
    val availableTask = if (innerTaskSize == 0) innerExtraSize else poolSize
    
    Engine.default.invokeAndWait2((0 until availableTask).map(tid => () => {
      val innerStart = tid * innerTaskSize + math.min(innerExtraSize, tid)
      val innerLength = innerTaskSize + (if (tid < innerExtraSize) 1 else 0)
      var i = 1
      while (i < gradientBuffer.length) {
        gradientBuffer(0).narrow(1, innerStart + 1, innerLength)
            .add(gradientBuffer(i).narrow(1, innerStart + 1, innerLength))
        i += 1
      }
      tid
    }))

    val parameterBuffer = new FP16SplitsCompressedTensor[T](gradientBuffer(0).nElement(),
      executorNum).asInstanceOf[CompressedTensor[T]]
    parameterBuffer.compress(gradientBuffer(0))
    var pid = 0
    val taskSize = gradientBuffer(0).nElement() / executorNum
    val extraSize = gradientBuffer(0).nElement() % executorNum
    while (pid < executorNum) {
      val start = pid * taskSize + math.min(pid, extraSize)
      val length = taskSize + (if (pid < extraSize) 1 else 0)
      val blockId = getGradientBlockId(executorId, pid.toString)
      BlockManagerWrapper.putBytes(
        blockId, parameterBuffer.bytes(start, length),
        StorageLevel.MEMORY_ONLY_SER)
      pid += 1
    }
  }
  
  def aggregateGlobalGradient(params: Array[CompressedTensor[T]]) : Unit = {
    val bm = SparkEnv.get.blockManager
    val sgThreads = (0 until executorNum).map(pid => {
      new Callable[Int] {
        override def call(): Int = {
          try {
            val blockId = getGradientBlockId(pid.toString, executorId)
            val tmp = BlockManagerWrapper.byteBufferConvert(bm.getLocalBytes(blockId)
              .getOrElse(bm.getRemoteBytes(blockId).get))
            params(pid) = SerializerInstance.serialize(tmp)
            BlockManagerWrapper.unlock(blockId)
            pid
          } catch {
            case t : Throwable =>
              logger.error("Error: " + ExceptionUtils.getStackTrace(t))
              throw t
          }
        }
      }
    })
    syncPool.invokeAll(sgThreads.asJava)
  }

  def aggregrateGradientParition2(params: Array[CompressedTensor[T]]): Unit = {
    val taskSize = gradientBuffer(0).nElement() / executorNum
    val extraSize = gradientBuffer(0).nElement() % executorNum
    val length = taskSize + (if (executorId.toInt < extraSize) 1 else 0)
    val poolSize = Engine.default.getPoolSize
    val innerTaskSize = length / poolSize
    val innerExtraSize = length % poolSize
    val availableTask = if (innerTaskSize == 0) innerExtraSize else poolSize
    Engine.default.invokeAndWait2((0 until availableTask).map(tid => () => {
      val innerStart = tid * innerTaskSize + math.min(innerExtraSize, tid)
      val innerLength = innerTaskSize + (if (tid < innerExtraSize) 1 else 0)
      params.reduce((l, r) => l.add(r.bytes(innerStart, innerLength), innerStart,
        innerLength))
      tid
    }))

    params.head.deCompress(gradientPartition)
  }
  
  def aggregateGlobalWeight() : Unit = {
    
  }

  def getGradientBlockId(pidFrom : String, pidTo : String): BlockId = {
    SparkExtension.getLocalBlockId("parameterManager" + pidTo + "gradientBytes" + pidFrom)
  }
}
