package com.jindanfenqi.spark.model

/**
  * 数据库表 cl_user
  *
  * @param id  userId
  * @param regist_time  注册时间
  * @param channel_id 渠道ID
  * @param source 渠道来源
  * @param gmt_modified 修改时间
  */
case class UserTableModel(id: Long, regist_time: String, channel_id: Long, source: Int, gmt_modified: String)
