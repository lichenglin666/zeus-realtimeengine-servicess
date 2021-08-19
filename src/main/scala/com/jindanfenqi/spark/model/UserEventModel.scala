package com.jindanfenqi.spark.model

import java.sql.Timestamp

/**
  * 用户事件
  *
  * @param userId 用户ID
  * @param ts  kafka topic 中带有的时间错
  * @param timestamp 用户表中对于的时间字段
  * @param channelId  渠道ID
  * @param event 事件类型
  */
case class UserEventModel(userId: Long, ts: Timestamp, timestamp: Timestamp, channelId: Long, event: String)
