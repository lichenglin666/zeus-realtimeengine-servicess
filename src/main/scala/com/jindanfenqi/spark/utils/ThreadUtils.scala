package com.jindanfenqi.spark.utils

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
  * 线程工具
  */
object ThreadUtils {

  /**
    * 单线程
    */
  val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  def submit(task: Runnable): Unit = {
    executor.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES)
  }

  /**
    * 周期性的执行任务
    */
  def submit(task: Runnable, period: Long, timeUnit: TimeUnit): Unit = {
    executor.scheduleAtFixedRate(task, period, period, timeUnit)
  }

  /**
    * 关闭线程池
    */
  def  shutdown(): Unit = {
    if (! executor.isShutdown) {
      executor.shutdown()
    }
  }

}
