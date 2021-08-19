package com.jindanfenqi.spark.model

/**
  * cl_new_user_auth 表对应的数据模型
  */
case class NewUserAuthTableModel(id: Long, user_id: Long, auth_item_code: String, state: String, gmt_create: String, gmt_modified: String)