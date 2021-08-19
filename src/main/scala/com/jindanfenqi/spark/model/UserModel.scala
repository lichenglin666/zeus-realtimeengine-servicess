package com.jindanfenqi.spark.model

import java.sql.Timestamp
import java.time.LocalDate


/**
  *  用户注册信息
  */
case class UserModel(id: Long, registerTime: Timestamp, channelId: Long, source: Int, modifiedTime: Timestamp) {

  /**
    * 是否是当天注册用户
    *
    * @return
    */
  def isCurrentDateUser = registerTime.toLocalDateTime.toLocalDate.isEqual(LocalDate.now())
}
