package com.jindanfenqi.spark.model

import java.sql.Timestamp


/**
  * maxwell 拉取 binlog 到 kafka topic 中,
  * topic 记录的 value 字段中含有 binlog 数据
  *
  * @param key
  * @param value  包含 binlog 数据， json line
  * @param topic
  * @param partition
  * @param offset
  * @param timestamp
  * @param timestampType
  */
case class KafkaModel(key: String, value: String, topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType: Int)


