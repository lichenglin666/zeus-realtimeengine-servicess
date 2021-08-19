package com.jindanfenqi.spark.model

import java.sql.Timestamp

import scala.collection.mutable

/**
  * 用户内部状态
  */
case class NewUserStateModel(userId: Long, ts: Timestamp, channelId: Long, flags: mutable.HashSet[String])
