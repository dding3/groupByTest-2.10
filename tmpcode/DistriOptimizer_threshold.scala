/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package com.intel.analytics.bigdl.optim

import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.dataset.{DataSet => DataSource, Batch, DistributedDataSet}
import com.intel.analytics.bigdl.parameters.{CompressedTensor, AllReduceParameter, AllReduceParameterManager, ParameterManager}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils._
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.reflect.ClassTag

object DistriOptimizer {
  import Optimizer._

  private var lossArray: Array[Double] = null
  private var recordsArray: Array[Int] = null
  private var tasks: ArrayBuffer[Future[_]] = new ArrayBuffer()

  val logger = Logger.getLogger(getClass)
  
  /**
   * Optimizer cache some metadata on each executor
   *
   * @param localModels cached models
   * @param modelWeights weights of the cached models
   * @param modelGradients gradients of the cached models
   * @param localCriterions cached criterion
   * @param localStates cached state
   * @param buffer tensor buffer
   * @tparam T
   */
  case class Cache[T](
    localModels: Array[Module[T]],
    modelWeights: Array[Tensor[T]],
    modelGradients: Array[Tensor[T]],
    localCriterions: Array[Criterion[T]],
    localStates: Array[Table],
    buffer: Tensor[T],
    var moduleTimeList: Array[Long] = null
  )

  private[optim] def optimize[T: ClassTag](
    dataset: DistributedDataSet[Batch[T]],
    coresPerNode: Int,
    state: Table,
    endWhen: Trigger,
    metrics: Metrics,
    models: RDD[Cache[T]],
    model: Module[T],
    optimMethod: OptimMethod[T],
    validationTrigger: Option[Trigger],
    validationDataSet: Option[DataSource[RDD[Batch[T]]]],
    validationMethods: Option[Array[ValidationMethod[T]]],
    cacheTrigger: Option[Trigger],
    cachePath: Option[String],
    isOverWrite: Boolean
  )(implicit ev: TensorNumeric[T]) = {
    val sc = dataset.data(looped = true).sparkContext
    val partitionNum = dataset.originRDD().partitions.length
    var wallClockTime = 0L
    val driverState = T("epoch" -> state.get[Int]("epoch").getOrElse(1),
      "neval" -> state.get[Int]("neval").getOrElse(1))
    val _subModelNumber = Engine.getEngineType match {
      case MklBlas => coresPerNode
      case MklDnn => 1
      case _ => throw new IllegalArgumentException()
    }
    var accumulateCount = 0
    val shuffleBefore = System.nanoTime()
    logger.info(s"config $state")
    logger.info(s"Shuffle data")
    dataset.shuffle()
    val shuffleEnd = System.nanoTime()
    logger.info(s"Shuffle data complete. Takes ${(shuffleEnd - shuffleBefore) / 1e9}s")

    var threshold = 0L
    var timeout = Long.MaxValue
    var iteration = 0
    val dropPercentage = state.get[Double]("dropPercentage").get
    val iterationIgnoredNum = state.get[Int]("iterationIgnoredNum").get
    val comupteThresholdbatchSize = state.get[Int]("comupteThresholdbatchSize").get
    val driverSubModelNum = partitionNum * _subModelNumber
    val _ps = new AllReduceParameter[T]()
    
    var epochStart = System.nanoTime()
    while (!endWhen(driverState)) {
      val _header = header(driverState[Int]("epoch"), accumulateCount, dataset.size(),
        driverState[Int]("neval"), wallClockTime)
      val lossSum = sc.accumulator(0.0, "loss sum")
      val recordsNum = sc.accumulator(0, "record number")
      metrics.set("computing time for each node", mutable.ArrayBuffer[Double](), sc)
      metrics.set("computing time average", 0.0, sc, partitionNum)
      metrics.set("aggregate gradient time", 0.0, sc, partitionNum)

      metrics.set("task1 time from worker", mutable.ArrayBuffer[Double](), sc)
      metrics.set("task2 time from worker", mutable.ArrayBuffer[Double](), sc)
      metrics.set("task1 time from driver", mutable.ArrayBuffer[Double](), sc)
      metrics.set("task2 time from driver", mutable.ArrayBuffer[Double](), sc)
      metrics.set("get weights average", 0.0, sc, partitionNum)
      metrics.set("get weights for each node", mutable.ArrayBuffer[Double](), sc)
      metrics.set("get gradients average", 0.0, sc, partitionNum)
      metrics.set("get gradients for each node", mutable.ArrayBuffer[Double](), sc)

      val driverMetrics = metrics
      val start = System.nanoTime()
      val finishedModelNum = dataset.data(looped = true).zipPartitions(
        models, true)(
        (data, modelIter) => {
          val task1WorkStart = System.nanoTime()
          val cached = modelIter.next()
          val syWStart = System.nanoTime()
          val getWeightsTasks = _ps.getWeights(cached.modelWeights.head, partitionNum)
          val tensorBuffer = new Array[(Tensor[T], Tensor[T])](_subModelNumber)
          tasks += Engine.default.invoke(() => {
            val batch = data.next()
            var b = 0
            require(batch.data.size(1) == batch.labels.size(1))
            val stackSize = batch.data.size(1) / _subModelNumber
            while (b < _subModelNumber) {
              tensorBuffer(b) = (batch.data.narrow(1, b * stackSize + 1, stackSize),
                batch.labels.narrow(1, b * stackSize + 1, stackSize))
              b += 1
            }
          })
          Engine.default.sync(tasks)
          getWeightsTasks.foreach(_.get())
          val weightSyncTime = System.nanoTime()-syWStart
          driverMetrics.add("get weights average", weightSyncTime)
          driverMetrics.add("get weights for each node", weightSyncTime)
          tasks.clear()

          

          if (lossArray == null || lossArray.length < _subModelNumber) {
            lossArray = new Array[Double](_subModelNumber)
          }

          if (recordsArray == null || recordsArray.length < _subModelNumber) {
            recordsArray = new Array[Int](_subModelNumber)
          }

          // ======================Start train models===================================
          var time = System.nanoTime()
          if(iteration > iterationIgnoredNum + comupteThresholdbatchSize - 1) {
            timeout = threshold - weightSyncTime
          }
          val pre = (iteration % comupteThresholdbatchSize) * _subModelNumber
          val trainingThreads = Engine.default.invokeAndWait2((0 until _subModelNumber).map(i =>
            () => {
              val trainStart = System.nanoTime()
              val localModel = cached.localModels(i)
              localModel.training()
              val localCriterion = cached.localCriterions(i)
              val (input, target) = tensorBuffer(i)
              val output = localModel.forward(input)
              lossArray(i) = ev.toType[Double](localCriterion.forward(output, target))
              val errors = localCriterion.backward(output, target)
              localModel.backward(input, errors)
              recordsArray(i) = target.size(1)
              cached.moduleTimeList(i + pre) = System.nanoTime() - trainStart + weightSyncTime
              i
            }
          ), timeout)
          val computingTime = System.nanoTime() - time
          driverMetrics.add("computing time average", computingTime)
          driverMetrics.add("computing time for each node", computingTime)

          val finishedThreads = trainingThreads.filter(!_.isCancelled).map(_.get())
          var i = 0
          while (i < finishedThreads.size) {
            lossSum += lossArray(finishedThreads(i))
            recordsNum += recordsArray(finishedThreads(i))
            i += 1
          }
          
          cached.buffer.zero()
          if (finishedThreads.size > 0) {
            time = System.nanoTime()
            val gradLength = cached.modelGradients(0).nElement()
            val taskSize = gradLength / _subModelNumber
            val extraTask = gradLength % _subModelNumber

            // copy multi-model gradient to the buffer
            val parallelNum = if (taskSize == 0) extraTask else _subModelNumber
            Engine.default.invokeAndWait((0 until parallelNum).map(tid => () => {
              val offset = tid * taskSize + math.min(tid, extraTask)
              val length = taskSize + (if (tid < extraTask) 1 else 0)
              var i = 0
              while (i < finishedThreads.length) {                
                  cached.buffer.narrow(1, offset + 1, length)
                    .add(cached.modelGradients(finishedThreads(i)).narrow(1, offset + 1, length))
                i += 1
              }
            }))
            driverMetrics.add("aggregate gradient time", System.nanoTime() - time)
          }
          _ps.putGradients(cached.buffer, partitionNum)
          tasks ++= Engine.default.invoke((0 until _subModelNumber).map(i => () => {
            cached.localModels(i).training()
            cached.localModels(i).zeroGradParameters()
          }))
          driverMetrics.add("task1 time from worker", System.nanoTime() - task1WorkStart)    
          Iterator(finishedThreads.size)
        }).reduce(_ + _)
      metrics.add("task1 time from driver", System.nanoTime() - start)

      if (finishedModelNum > driverSubModelNum * 0.5) {        
        val task2Start = System.nanoTime()
        val value = lossSum.value / finishedModelNum
        models.mapPartitions(modelIter => {
          val task2WorkerStart = System.nanoTime()
          val modelCache = modelIter.next()
          val params = new Array[CompressedTensor[T]](partitionNum)
          val getGstart = System.nanoTime()
          val getGradients = _ps.getGradients(params, partitionNum)
          getGradients.foreach(_.get())
          val getGTime = System.nanoTime() - getGstart
          driverMetrics.add("get gradients average", getGTime)
          driverMetrics.add("get gradients for each node", getGTime)
          params.head.deCompress(_ps.partialGradients)
          _ps.partialGradients.div(ev.fromType(finishedModelNum))
          modelCache.localStates.head("neval") = driverState[Int]("neval")
          modelCache.localStates.head("epoch") = driverState[Int]("epoch")
          optimMethod.optimize(_ => (ev.fromType(value), _ps.partialGradients),
            _ps.partialWeights, modelCache.localStates.head, modelCache.localStates.head)

          _ps.putWeights()
          driverMetrics.add("task2 time from worker", System.nanoTime() - task2WorkerStart)
          Iterator.empty
        }).count()
        metrics.add("task2 time from driver", System.nanoTime()-task2Start)

        accumulateCount += recordsNum.value
        val end = System.nanoTime()
        logger.info(s"${_header} Train ${recordsNum.value} in ${(end - start) / 1e9}seconds. " +
          s"Throughput is ${recordsNum.value / ((end - start) / 1e9)} records/second. Loss is ${
            lossSum.value / finishedModelNum
          }. ")
        logger.info("\n" + metrics.summary())
        logger.info("Dropped modules: " + (driverSubModelNum - finishedModelNum))

        // compute threshold
        iteration += 1
        if (iteration > iterationIgnoredNum && iteration % comupteThresholdbatchSize == 0) {
          val moduleTimeList = models.mapPartitions { iter =>
            iter.next().moduleTimeList.iterator
          }.collect()

          val k = (dropPercentage * comupteThresholdbatchSize * driverSubModelNum).toInt
          val dropModelsNum = driverSubModelNum - finishedModelNum
          if (k > dropModelsNum) {
            threshold = Util.kthLargest(moduleTimeList, 0, moduleTimeList.length-1, k - dropModelsNum)  
          } else {
            threshold = (threshold * 1.01).toLong
          }          
          logger.info("threshold: " + threshold)

          // clear moduleTimeList in each node
          models.mapPartitions { iter => iter.next.moduleTimeList =
            new Array[Long](_subModelNumber * comupteThresholdbatchSize)
            Iterator.empty
          }.count()
        }
        
        driverState("neval") = driverState[Int]("neval") + 1
        if (accumulateCount >= dataset.size()) {
          val epochEnd = System.nanoTime()
          wallClockTime = wallClockTime + epochEnd - epochStart
          epochStart = System.nanoTime()
          logger.info(s"${_header} Epoch finished. Wall clock time is ${wallClockTime / 1e6}ms")

          driverState("epoch") = driverState[Int]("epoch") + 1
          dataset.shuffle()
          accumulateCount = 0
        }
        validate(
          validationTrigger,
          validationDataSet,
          validationMethods,
          coresPerNode,
          models,
          wallClockTime,
          driverState
        )

        checkpoint(
          cacheTrigger,
          cachePath,
          isOverWrite,
          wallClockTime,
          models,
          model,
          driverState
        )
      } else {
        logger.info(s"Warning!!! Ignore this iteration as more than half " +
          s"module is dropped!! Finished modules number: ${finishedModelNum}")
      }
    }
  }


  private def checkpoint[T: ClassTag](
    cacheTrigger: Option[Trigger],
    cachePath: Option[String],
    isOverWrite: Boolean,
    wallClockTime: Long,
    models: RDD[Cache[T]],
    model : Module[T],
    state: Table)
  : Unit = {
    if (cacheTrigger.isDefined) {
      val trigger = cacheTrigger.get
      if (trigger(state) && cachePath.isDefined) {
        println(s"[Wall Clock ${wallClockTime / 1e9}s] Save model to ${cachePath.get}")
        saveModel(getModel(models, model), cachePath, isOverWrite, s".${state[Int]("neval")}")
        saveState(models.map(_.localStates.head).first(), cachePath, isOverWrite, s"" +
          s".${state[Int]("neval")}")
      }
    }
  }

  private def initThreadModels[T: ClassTag](
    model: Module[T],
    dataset: DistributedDataSet[Batch[T]],
    criterion: Criterion[T],
    state: Table,
    nodeNumber: Int,
    coresPerNode: Int,
    checkSingleton: Boolean
  )(implicit ev: TensorNumeric[T]) = {
    val sc = dataset.originRDD().sparkContext
    val broadcast = sc.broadcast((model, criterion, state))
    val _subModelNumber = Engine.getEngineType match {
      case MklBlas => coresPerNode
      case MklDnn => 1
    }

    require(dataset.originRDD().partitions.length == nodeNumber,
      s"Passed in rdd partition number ${dataset.originRDD().partitions.length}" +
        s" is not equal to configured node number ${nodeNumber}")

    val partitionNum = dataset.originRDD().partitions.length
    val comupteThresholdbatchSize = state.get[Int]("comupteThresholdbatchSize").get
    val _ps = new AllReduceParameter[T]()
    val models = dataset.originRDD().mapPartitions(_ => {
      val (broadcastModel, broadcastCriterion, broadcastState) = broadcast.value
      if (checkSingleton) {
        require(Engine.checkSingleton(), "Detect multi-task run on one Executor/Container. " +
          "Currently not support this")
      }
      val cached = (0 until _subModelNumber).map { _ =>
        val localModel = broadcastModel.cloneModule()
        val localCriterion = broadcastCriterion.cloneCriterion()
        val localState = broadcastState.clone()
        val (weights, grads) = localModel.getParameters()
        (localModel, weights, grads, localCriterion, localState)
      }.toArray

      val weights = cached.head._2
      cached.map(c =>
        if (!c._2.eq(weights)) {
          c._2.storage().set(weights.storage())
        }
      )
      AllReduceParameter.taskSize = weights.nElement() / partitionNum
      AllReduceParameter.extraSize = weights.nElement() % partitionNum
      AllReduceParameter.tlength = weights.nElement()
      AllReduceParameter.partitionNum = partitionNum
      _ps.init(weights)
      

      Iterator(Cache(
        cached.map(_._1), // models
        cached.map(_._2), // weights
        cached.map(_._3), // gradients
        cached.map(_._4), // criterions
        cached.map(_._5), // states
        cached.head._2.clone(), // a tensor buffer
        new Array[Long](_subModelNumber * comupteThresholdbatchSize)
      ))
    }).persist()
    models.setName("Thread Model RDD")
    logger.info("Cache thread models...")
    models.count()
    logger.info("Cache thread models... done")
    models
  }


  private def validate[T](
    validationTrigger: Option[Trigger],
    validationDataSet: Option[DataSource[RDD[Batch[T]]]],
    validationMethods: Option[Array[ValidationMethod[T]]],
    coresPerNode: Int,
    models: RDD[Cache[T]],
    wallClockTime: Long,
    state: Table
  ): Unit = {
    if (validationTrigger.isEmpty || validationDataSet.isEmpty) {
      return
    }
    val trigger = validationTrigger.get
    if (!trigger(state)) {
      return
    }
    val vMethods = validationMethods.get
    val validateRDD = validationDataSet.get.data(looped = false)
    logger.info(s"[Wall Clock ${wallClockTime / 1e9}s] Validate model...")
    val _subModelNumber = Engine.getEngineType match {
      case MklBlas => coresPerNode
      case MklDnn => 1
    }
    models.zipPartitions(validateRDD)((modelIter, dataIter) => {
      val workingModels = modelIter.next().localModels
      workingModels.foreach(_.evaluate())
      dataIter.map(batch => {
        require(batch.data.size(1) == batch.labels.size(1))
        val stackSize = batch.data.size(1) / _subModelNumber
        val extraSize = batch.data.size(1) % _subModelNumber
        val parallelism = if (stackSize == 0) extraSize else _subModelNumber
        Engine.default.invokeAndWait(
          (0 until parallelism).map(b =>
            () => {
              val offset = b * stackSize + math.min(b, extraSize)
              val length = stackSize + (if (b < extraSize) 1 else 0)
              val input = batch.data.narrow(1, offset + 1, length)
              val target = batch.labels.narrow(1, offset + 1, length)
              val output = workingModels(b).forward(input)
              vMethods.map(validation => {
                validation(output.asInstanceOf[Tensor[T]], target)
              })
            }
          )
        ).reduce((left, right) => {
          left.zip(right).map { case (l, r) =>
            l + r
          }
        })
      })
    }).reduce((left, right) => {
      left.zip(right).map { case (l, r) =>
        l + r
      }
    }).zip(vMethods).foreach(r => {
      logger.info(s"${r._2} is ${r._1}")
    })
  }


  private def getModel[T: ClassTag](
    models: RDD[Cache[T]], model : Module[T]): Module[T] = {
    val _ps = new AllReduceParameter[T]()
    val partitionNum = models.partitions.length
    val weights = models.mapPartitions(iter => {
      val cached = iter.next()
      val curPartitionId = TaskContext.getPartitionId()
      Iterator.single(Map(curPartitionId -> _ps.partialWeights))
    }).reduce(_ ++ _)

    val parameter = model.getParameters()._1
    val parameterLength = parameter.nElement()
    val taskSize = parameterLength / partitionNum
    require(taskSize != 0, "parameter length should not less than partition number")
    val extraSize = parameterLength % partitionNum

    (0 until partitionNum).map(pid => {
      val start = pid * taskSize + math.min(pid, extraSize)
      val length = taskSize + (if (pid < extraSize) 1 else 0)
      parameter.narrow(1, start + 1, length).copy(weights(pid))
    })

    model
  }
}

class DistriOptimizer[T: ClassTag](
  model: Module[T],
  dataset: DistributedDataSet[Batch[T]],
  criterion: Criterion[T]
)
  (implicit ev: TensorNumeric[T])
  extends Optimizer[T, RDD[Batch[T]], RDD[Batch[T]]](
    model, dataset, criterion) {

  def disableCheckSingleton(): this.type = {
    this.checkSingleton = false
    this
  }

  private var checkSingleton = true

  val metrics = new Metrics

  private var models: RDD[DistriOptimizer.Cache[T]] = null

  override def optimize(): Module[T] = {
    optimMethod.clearHistory(state)
    state("dropPercentage") = dropPercentage
    state("iterationIgnoredNum") = ignoreIterationNum
    state("comupteThresholdbatchSize") = comupteThresholdbatchSize    

    require(Engine.nodeNumber().isDefined, "Node number is not set")
    val nodeNumber = Engine.nodeNumber().get
    val coresPerNode = Engine.coreNumber()

    models = DistriOptimizer.initThreadModels(
      model, dataset, criterion, state, nodeNumber, coresPerNode, checkSingleton)

    DistriOptimizer.optimize(
      dataset,
      coresPerNode,
      state,
      endWhen,
      metrics,
      models,
      model,
      optimMethod,
      validationTrigger,
      validationDataSet,
      validationMethods,
      cacheTrigger,
      cachePath,
      isOverWrite
    )

    DistriOptimizer.getModel(models, model)
  }
}


