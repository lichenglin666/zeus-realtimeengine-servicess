package com.jindanfenqi.spark.model

/**
  * 用户事件类型
  */
object UserEventType extends Serializable {
  /**
    * 注册事件
    */
  val NEW_USER_REGISTER = "NEW_USER_REGISTER"
  val TOTAL_NEW_USER_REGISTER = "TOTAL_NEW_USER_REGISTER"

  /**
    * 认证事件
    */
  val ALIPAY = "ALIPAY"
  val BANK_DEPOSITORY = "BANK_DEPOSITORY"
  val BIND_BANK_CARD = "BIND_BANK_CARD"
  val EMAIL = "EMAIL"
  val EMERGENCY_CONTACT = "EMERGENCY_CONTACT"
  val ID_INFO = "ID_INFO"
  val PHONE_OPERATOR = "PHONE_OPERATOR"

  /**
    * 登录事件
    */
  val APP_SESSION = "APP_SESSION"
}
