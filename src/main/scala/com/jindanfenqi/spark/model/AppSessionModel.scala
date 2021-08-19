package com.jindanfenqi.spark.model

import java.sql.Timestamp
import java.time.LocalDate


/**
  * 用户 App 会话 model
  *
  * @param userId  用户 ID
  * @param lastAccessTime  用户会话最后更新时间
  * @param status  用户会话状态
  */
case class AppSessionModel(userId: Long, lastAccessTime: Timestamp, status: Int) {

  /**
    * 是用户当天访问
    *
    * @return
    */
  def isCurrentDateAppSession = lastAccessTime.toLocalDateTime.toLocalDate.equals(LocalDate.now())
}
