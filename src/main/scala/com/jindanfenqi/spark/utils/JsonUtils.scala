package com.jindanfenqi.spark.utils

import java.sql.Timestamp

import com.jindanfenqi.spark.model._
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._


/**
  * Json 数据解析工具
  */
object JsonUtils {

  implicit val formats = DefaultFormats


  def toUserModel(value: String): UserModel = {
    read[UserModel](value)
  }


  def toNewUserAuthModel(value: String): NewUserAuthModel = {
    read[NewUserAuthModel](value)
  }


  /**
    *  解析 json 字符串成对象
    */
  def toUserTableModel(value: String): UserModel = {
    val user = read[UserTableModel](value)
    UserModel(user.id, Timestamp.valueOf(user.regist_time), user.channel_id, user.source, Timestamp.valueOf(user.gmt_modified))
  }


  /**
    * 解析 json 字符串成对象
    */
  def toNewUserAuthTableModel(value: String): NewUserAuthModel = {
    val auth = read[NewUserAuthTableModel](value)
    NewUserAuthModel(auth.id, auth.user_id, auth.auth_item_code, auth.state, Timestamp.valueOf(auth.gmt_create), Timestamp.valueOf(auth.gmt_modified))
  }


  def toAppSessionTableModel(value: String): AppSessionModel = {
    val appSession = read[AppSessionTableModel](value)
    AppSessionModel(appSession.user_id, Timestamp.valueOf(appSession.last_access_time), appSession.status)
  }
}
