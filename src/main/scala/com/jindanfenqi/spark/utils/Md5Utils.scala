/*
 * Copyright (c) 2010-2018 www.co-mall.com/ Inc. All rights reserved.
 * 注意：本内容仅限于北京科码先锋互联网技术股份有限公司内部传阅，禁止外泄以及用于其他商业目的。
 */
package com.jindanfenqi.spark.utils

import java.security.MessageDigest

object Md5Utils {

  val md5 = MessageDigest.getInstance("MD5")

  def hash(str: String): String = {
    md5.digest(str.getBytes).map("%02x".format(_)).mkString
  }

}
