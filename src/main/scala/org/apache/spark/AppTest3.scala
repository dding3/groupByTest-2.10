package org.apache.spark

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

/**
  * Created by ding on 10/17/16.
  */
object AppTest3 {
  def main(args: Array[String]): Unit = {
    var sum = 0.0
    val executor2 = Executors.newFixedThreadPool(4);
    val context2 = new ExecutionContext {
      val threadPool = executor2

      def execute(runnable: Runnable) {
        threadPool.submit(runnable)
      }

      def reportFailure(t: Throwable) {}
    }

//    val executorInternal = Executors.newFixedThreadPool(8);
//    val contextInternal = new ExecutionContext {
//      val threadPool = executorInternal
//
//      def execute(runnable: Runnable) {
//        threadPool.submit(runnable)
//      }
//
//      def reportFailure(t: Throwable) {}
//    }
    
    for (i <- 0 until 50) {
      val executorInternal = Executors.newFixedThreadPool(8);
      val contextInternal = new ExecutionContext {
        val threadPool = executorInternal

        def execute(runnable: Runnable) {
          threadPool.submit(runnable)
        }

        def reportFailure(t: Throwable) {}
      }
      
      val start = System.nanoTime()
      (0 until 8).map(i => Future {
        System.out.println(Thread.currentThread().getName() + " Start. Command = " + i);
        System.out.println(Thread.currentThread().getName() + " Start. Command = " + i);
        System.out.println(Thread.currentThread().getName() + " Start. Command = " + i);
        Thread.sleep(2000);
        System.out.println(Thread.currentThread().getName() + " End.");
        System.out.println(Thread.currentThread().getName() + " End.");
        System.out.println(Thread.currentThread().getName() + " End.");
      }(contextInternal)).foreach(Await.result(_, Duration.Inf))
      sum += (System.nanoTime() - start).toDouble / 1e9

      (0 until 4).map(i => Future {
        System.out.println(Thread.currentThread().getName() + " Start. Command = " + i);
        System.out.println(Thread.currentThread().getName() + " Start. Command = " + i);
        Thread.sleep(2000);
        System.out.println(Thread.currentThread().getName() + " End.");
        System.out.println(Thread.currentThread().getName() + " End.");
      }(context2)).foreach(Await.result(_, Duration.Inf))

    }

    println("time: " + sum)
  }
}

class WorkerThread extends Runnable {

  var command = ""

  def this(s: String) = {
    this
    command=s    
  }   
  override def run() = {
  
}

  def processCommand() = { 
  Thread.sleep(5000);
}

  
  override def toString() = {
  this.command}
}
