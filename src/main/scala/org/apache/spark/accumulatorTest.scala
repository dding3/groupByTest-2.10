package org.apache.spark
import akka.pattern.after
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors}

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import akka.actor.ActorSystem
import akka.pattern.after
import akka.actor.ActorSystem

import scala.collection.mutable.ArrayBuffer


//import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import scala.concurrent.{Await, ExecutionContext, Future}
//import scala.util.{Failure, Success}
import java.util.concurrent.Future

import scala.concurrent.ExecutionContext.Implicits.global
/**
  * Created by ding on 9/23/16.
  */

import org.jboss.netty.util.{HashedWheelTimer, TimerTask, Timeout}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.Promise
import java.util.concurrent.TimeoutException
import scala.util.{ Success, Failure }


//object TimeoutScheduler{
//  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
//  def scheduleTimeout(promise:Promise[_], after:Duration) = {
//    timer.newTimeout(new TimerTask{
//      def run(timeout:Timeout){
//        promise.failure(new TimeoutException("Operation timed out after " + after.toMillis + " millis"))
//      }
//    }, after.toNanos, TimeUnit.NANOSECONDS)
//  }
//}
  
object accumulatorTest {

//  def withTimeout[T](fut:Future[T])(implicit ec:ExecutionContext, after:Duration) = {
//def withTimeout[T](fut:Future[T], after:Duration)(implicit ec:ExecutionContext) = {
//    val prom = Promise[T]()
//    val timeout = TimeoutScheduler.scheduleTimeout(prom, after)
//    val combinedFut = Future.firstCompletedOf(List(fut, prom.future))
//    fut onComplete{case result => timeout.cancel()}
//    combinedFut
//  }

  
  def main(args: Array[String]): Unit = {
    
//    (0 until 5).map(i => scala.concurrent.Future {     
//      Thread.sleep(i * 1000)
//      println("hello " + i)
//    }).foreach(Await.result(_, Duration(1, "s")))
    
    
    
//    val thread1 = new Thread(new Runnable {
//      override def run(): Unit =  {
//        println("T1")
//        Thread.sleep(1000)
//      }
//    })
//    val thread2 = new Thread(new Runnable {
//      override def run(): Unit =  {
//        println("T2")
//        Thread.sleep(1000)
//      }
//    })
//    val thread3 = new Thread(new Runnable {
//      override def run(): Unit =  {
//        println("T3")
//        Thread.sleep(1000)
//      }
//    })
//    val thread4 = new Thread(new Runnable {
//      override def run(): Unit =  {
//        println("T4")
//        Thread.sleep(1000)
//      }
//    })
//
//    thread1.start();
//    thread1.join();
//    thread2.start();
//    thread2.join();
//    thread3.start();
//    thread3.join();
//    thread4.start();
//    thread4.join();
//
//    val thread5 = new Thread(new Runnable {
//      override def run(): Unit =  {
//        println("T5")
//        Thread.sleep(1000)
//      }
//    })
//    val thread6 = new Thread(new Runnable {
//      override def run(): Unit =  {
//        println("T6")
//        Thread.sleep(1000)
//      }
//    })
//    val thread7 = new Thread(new Runnable {
//      override def run(): Unit =  {
//        println("T7")
//        Thread.sleep(1000)
//      }
//    })
//    val thread8 = new Thread(new Runnable {
//      override def run(): Unit =  {
//        println("T8")
//        Thread.sleep(1000)
//      }
//    })
//
//    thread5.start();
//    thread6.start();
//    thread7.start();
//    thread8.start();
    
    
//    
    
//    
    val test2 = Array(1,2,3,4,5)
    val callables = (0 to 4).map(pid =>
    new Callable[Int] {
      def call(): Int =  {        
test2(pid) = pid * 10000
        val f = scala.concurrent.Future {
          Thread.sleep(pid * 1000)
          println(pid)
          
        }
        Await.result(f, Duration.Inf)
        test2(pid) = pid * 10000 + 1
       pid
      }
    })
    import collection.JavaConversions._
//
    
val executor = Executors.newFixedThreadPool(5)
////    val test = executor.invokeAll(callables.toArray.toSeq, -1, TimeUnit.MICROSECONDS)
//    val test = (2 to 6).map(pid =>
//      executor.submit(new Callable[Int] {
//        def call(): Int =  {
//          Thread.sleep(pid*1000)
//          println(pid)
//          pid
//        }
//      }))
//
//    println("futur get: " + test.foreach(_.get))

//    (0 to 6).map(pid => scala.concurrent.Future {
//    val n = test2(pid)  
//    })

    
        executor.invokeAll(callables, 300, TimeUnit.MILLISECONDS); // Timeout of 10 minutes.
        print(test2.mkString(" "))
    
      
    
        
    executor.shutdown();
    
//    implicit class FutureExtensions[T](f: Future[T]) {
//      def withTimeout(timeout: => Throwable)(implicit duration: FiniteDuration, system: ActorSystem): Future[T] = {
//        Future firstCompletedOf Seq(f, after(duration, system.scheduler)(Future.failed(timeout)))
//      }
//    }
//    implicit val system = ActorSystem("theSystem")
//    implicit val timeout = 1000 millisecond
//       

//    f.foreach(x => x withTimeout new TimeoutException("Future timed out!") onComplete {
//      case scala.util.Success(x) => println(x)
//      case Failure(error) => println(error)
//    })
    
  
 
//    val system = ActorSystem("theSystem")
//
//    val f = (0 to 8).map { i =>
//      Future {
//        Thread.sleep(5000); true
//      }
//    }
//    val futureList = Future.traverse((1 to 100).toList.map(i => Future {
//      i * 2 - 1
//    }))
//
//    val oddSum = futureList.map(_.sum)
//    oddSum foreach println
    
//    val f_0 = Future {
//      Thread.sleep(2000); true
//    }
//    lazy val t = after(duration = Duration(1,"second") , 
//      using = system.scheduler)(Future.failed(new scala.concurrent.TimeoutException("Future timed out!")))

//    val start = System.nanoTime()
//    val fWithTimeout = Future firstCompletedOf Seq(futureList, t)
//    val fWithTimeout = Future firstCompletedOf Seq(f_0, t)
//
//    val fWithTimeout1 = Future firstCompletedOf Seq(f(1), t)
//    val fWithTimeout2 = Future firstCompletedOf Seq(f(2), t)
    
//    f.map{ x =>
//      Future firstCompletedOf Seq(x, t)
//    }.foreach{x =>
//      x.onComplete {
//        case scala.util.Success(x) => println("success time: " + (System.nanoTime()-start.toDouble)/1e9)
//        case Failure(error) => println("error time: " + (System.nanoTime()-start.toDouble)/1e9)
//      }
//    }

//    fWithTimeout.onComplete {
//      case scala.util.Success(x) => println("success time: " + (System.nanoTime()-start.toDouble)/1e9)
//      case Failure(error) => println("error time: " + (System.nanoTime()-start.toDouble)/1e9)
//    }

//    fWithTimeout1.onComplete {
//      case scala.util.Success(x) => println("success time1: " + (System.nanoTime()-start.toDouble)/1e9)
//      case Failure(error) => println("error time1: " + (System.nanoTime()-start.toDouble)/1e9)
//    }
//
//    fWithTimeout2.onComplete {
//      case scala.util.Success(x) => println("success time2: " + (System.nanoTime()-start.toDouble)/1e9)
//      case Failure(error) => println("error time2: " + (System.nanoTime()-start.toDouble)/1e9)
//    }
    
  }
}
