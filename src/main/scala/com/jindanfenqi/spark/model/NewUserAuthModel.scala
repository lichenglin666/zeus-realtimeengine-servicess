package com.jindanfenqi.spark.model

import java.sql.Timestamp
import java.time.LocalDate

/**
  * 新用户认证模型
  *
  * authItemCode 取值：
  * ALIPAY 淘宝
  * BANK_DEPOSITORY  存管开户
  * BIND_BANK_CARD   绑卡
  * EMAIL  邮箱
  * EMERGENCY_CONTACT 联系人
  * ID_INFO 人脸
  * PHONE_OPERATOR 运营商
  *
  * state: 认证状态
  */

case class NewUserAuthModel(id: Long, userId: Long, authItemCode: String, state: String, createTime: Timestamp, modifiedTime: Timestamp) {
  /**
    * 是否是创建的认证记录
    *
    * @return
    */
  def isCurrentDateAuth = createTime.toLocalDateTime.toLocalDate.isEqual(LocalDate.now())

  /**
    * 认证状态是否是已经认证状态
    *
    * @return
    */
  def isAuthSuccess = state.equalsIgnoreCase("30")
}