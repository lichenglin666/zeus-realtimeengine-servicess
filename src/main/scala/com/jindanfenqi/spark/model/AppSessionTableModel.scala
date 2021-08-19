package com.jindanfenqi.spark.model

/**
  * 用户 App 会话表 model
  *
  * @param user_id 用户 ID
  * @param last_access_time 用户会话最后访问时间
  * @param status 用户会话最后状态
  */
case class AppSessionTableModel(user_id: Long, last_access_time: String, status: Int)
