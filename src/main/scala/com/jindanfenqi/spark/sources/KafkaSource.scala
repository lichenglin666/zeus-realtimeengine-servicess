package com.jindanfenqi.spark.sources

import com.jindanfenqi.spark.conf.KafkaConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSource {

  /**
    * 加载 kafka 数据源
    */
  def load(spark: SparkSession, kafkaOptions: Map[String, String] = KafkaConfig.topicConfig()): DataFrame = {
    spark.readStream.format("kafka").options(kafkaOptions).load
  }

}
