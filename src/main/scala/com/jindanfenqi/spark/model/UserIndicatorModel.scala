package com.jindanfenqi.spark.model

import java.sql.Timestamp


/**
  * 用户指标
  *
  * @param userId 用户ID
  * @param ts 时间戳
  * @param channelId 渠道ID
  * @param eventType 事件类型
  */
case class UserIndicatorModel(userId: Long, ts: Timestamp, channelId: Long, eventType: String)
