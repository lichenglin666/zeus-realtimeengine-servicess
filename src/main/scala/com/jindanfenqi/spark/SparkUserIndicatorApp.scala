package com.jindanfenqi.spark

import com.jindanfenqi.spark.conf.HbaseConfig
import com.jindanfenqi.spark.sources.{HbaseSources, KafkaSource}
import com.jindanfenqi.spark.stream._
import com.jindanfenqi.spark.utils.ThreadUtils
import org.apache.spark.internal.Logging

object SparkUserIndicatorApp extends App with Logging {
  val userIndicatorConfigName = "userIndicator"

  val spark = UserIndicatorSparkSession.spark
  spark.sparkContext.setLogLevel("WARN")
  val df = KafkaSource.load(spark)
  val kafkaModel = UserIndicatorStreamCompute.toKafkaModel(spark, df)
  val valueFieldModel = UserIndicatorStreamCompute.toValueField(spark, kafkaModel)
  val valueFieldModelFiltered = UserIndicatorStreamCompute.filterValueField(spark, valueFieldModel)
  val userEvent = UserIndicatorStreamCompute.toUserEvent(spark, valueFieldModelFiltered)
  val userIndicator = UserIndicatorStreamCompute.calcUserIndicatorModel(spark, userEvent)

  HbaseSources.save(
    userIndicator,
    HbaseConfig.toHbaseConfig(userIndicatorConfigName),
    HbaseConfig.checkpointLocation(userIndicatorConfigName),
    HbaseConfig.interval(userIndicatorConfigName)
  )

  ThreadUtils.submit(new CalcUserIndicatorTask(spark, userIndicatorConfigName))

  UserIndicatorStreamCompute.calcTotalNewUserQuantity(spark, userEvent)

  logDebug("spark structured streaming app started")
  spark.streams.awaitAnyTermination()
  ThreadUtils.shutdown()
}
