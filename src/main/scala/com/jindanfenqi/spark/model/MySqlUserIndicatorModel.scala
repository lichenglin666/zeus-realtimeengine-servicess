package com.jindanfenqi.spark.model

import java.sql.Timestamp


/**
  * MySql 表 user_indicator
  *
  * @param ts  时间
  * @param channelType 渠道类型
  * @param eventType 事件类型
  * @param quantity 数量
  */
case class MySqlUserIndicatorModel(ts: Timestamp, channelType: String, eventType: String, quantity: Long)