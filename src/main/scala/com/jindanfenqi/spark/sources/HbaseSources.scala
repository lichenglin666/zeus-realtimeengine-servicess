package com.jindanfenqi.spark.sources

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HbaseSources {

  private val HBASE_FORMAT = "org.apache.spark.sql.execution.datasources.hbase"

  def toStreamOptions(options: Map[String, String]): Map[String, String] = {
    options.map(kv => ("habse." + kv._1, kv._2))
  }


  def save[T](result: Dataset[T], options: Map[String, String], checkPointLocation: String,
              interval: String = "30 seconds"): StreamingQuery = {
    require(checkPointLocation.nonEmpty, "checkpoint location can't be empty")

    result.writeStream.format("hbase").
      option("checkpointLocation", checkPointLocation).
      options(toStreamOptions(options)).
      outputMode(OutputMode.Update()).
      trigger(Trigger.ProcessingTime(interval)).
      start
  }


  def load(spark: SparkSession, options: Map[String, String]): DataFrame = {
    spark.read.options(options).format(HBASE_FORMAT).load
  }


}
