package com.jindanfenqi.spark.utils

import java.sql.Timestamp

import com.jindanfenqi.spark.conf.JdbcConfig
import com.jindanfenqi.spark.sources.JdbcSink
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JdbcUtils {

  /**
    * 保存计算结果到数据库中
    */
  def save[T](ds: Dataset[T], jdbcOptions: Map[String, String] = JdbcConfig.tableConfig()): Unit = {
    ds.write.format("jdbc").options(jdbcOptions).mode("append").save
  }

  /**
    * 保存流计算结果到数据库中
    *
    */
  def saveStream(ds: Dataset[(String, String, Long, Timestamp)], interval: String = "1 seconds",
                  jdbcOptions: Map[String, String] = JdbcConfig.tableConfig()) = {
    val jdbcSink = new JdbcSink(jdbcOptions(JdbcConfig.URL), jdbcOptions(JdbcConfig.USER), jdbcOptions(JdbcConfig.PASSWORD))
    ds.writeStream.foreach(jdbcSink).outputMode(OutputMode.Complete()).trigger(Trigger.ProcessingTime(interval)).start
  }

  def load(spark: SparkSession, jdbcOptions: Map[String, String] = JdbcConfig.tableConfig()): DataFrame = {
    spark.read.format("jdbc").options(jdbcOptions).load
  }
}
