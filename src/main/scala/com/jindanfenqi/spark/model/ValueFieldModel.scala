package com.jindanfenqi.spark.model

import java.sql.Timestamp

import com.jindanfenqi.spark.utils.JsonUtils


/**
  * kafka topic 中 value 字段的模型
  *
  * @param database 数据库名称
  * @param table 表名称
  * @param operation 数据操作类型
  * @param timestamp 时间戳
  * @param data binlog 操作数据
  */
case class ValueFieldModel(database: String, table: String, operation: String, timestamp: Timestamp, data: String) {
  /**
    * binlog 记录中是否包含合法的用户
    *
    * 条件是当天新注册的用户
    * @return
    */
  def isValidUser = {
    if (table.equalsIgnoreCase("cl_user")) {
      val user = JsonUtils.toUserTableModel(data)
      isInsertOperation && user.isCurrentDateUser
    } else false
  }

  /**
    * binlog 记录中是否包含合法的认证记录
    *
    * 条件是当天认证并且认证状态是已认证
    * @return
    */
  def isValidAuth = {
    if (table.equalsIgnoreCase("cl_new_user_auth")) {
      val auth = JsonUtils.toNewUserAuthTableModel(data)
      isInsertOrUpdateOperation && auth.isAuthSuccess && auth.isCurrentDateAuth
    } else false
  }


  /**
    * binlog 记录中是否包含合法的 app session 记录
    *
    * @return
    */
  def isValidAppSession = {
    if (table.equalsIgnoreCase("cl_app_session")) {
      val appSession = JsonUtils.toAppSessionTableModel(data)
      isInsertOrUpdateOperation && appSession.isCurrentDateAppSession
    } else false
  }

  /**
    *  判断 binlog 记录的是否是 delete 操作
    *
    * @return
    */
  def isDeleteOperation = operation.equalsIgnoreCase("delete")

  def isInsertOperation = operation.equalsIgnoreCase("insert")

  def isUpdateOperation = operation.equalsIgnoreCase("update")

  /**
    * 判断 binlog 记录是否是 insert 或者 update 操作
    *
    * @return
    */
  def isInsertOrUpdateOperation = ! isDeleteOperation
}
