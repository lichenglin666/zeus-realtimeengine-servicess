package com.jindanfenqi.spark.conf

object JdbcConfig {
  private val config = ConfigManager.config.getConfig("jdbc")

  val URL = "url"
  val DRIVER = "driver"
  val TABLE = "dbtable"
  val USER = "user"
  val PASSWORD = "password"


  /**
    * 获取JDBC连接串的配置
    *
    * @param table 如果没有指定表名，从配置文件中读取
    * @return
    */
  def tableConfig(table: String = ""): Map[String, String] = {
    Map(
      URL -> config.getString(URL),
      DRIVER -> config.getString(DRIVER),
      TABLE -> {
        if(table.nonEmpty) table else config.getString(TABLE)
      },
      USER -> config.getString(USER),
      PASSWORD -> config.getString(PASSWORD)
    )
  }

}
