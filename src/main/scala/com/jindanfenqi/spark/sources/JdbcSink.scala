package com.jindanfenqi.spark.sources

import java.sql._
import java.util.Objects

import org.apache.spark.sql.ForeachWriter

/**
  * 写入 MySql 数据库 Sink 的简单实现
  */
class JdbcSink(url: String, user: String, password: String) extends ForeachWriter[(String, String, Long, Timestamp)] {
  val driverClass = "com.mysql.jdbc.Driver"
  var conn: Connection = _
  var statement: Statement = _
  var preparedStatement: PreparedStatement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driverClass)
    conn = DriverManager.getConnection(url, user, password)
    statement = conn.createStatement
    preparedStatement = conn.prepareStatement("insert into user_indicator values(?,?,?,?)")
    true
  }

  def process(value: (String, String, Long, Timestamp)): Unit = {
    preparedStatement.setString(1, value._1)
    preparedStatement.setString(2, value._2)
    preparedStatement.setLong(3, value._3)



    preparedStatement.setTimestamp(4, value._4)
    preparedStatement.executeUpdate()
  }

  def close(errorOrNull: Throwable): Unit = {
    if (Objects.nonNull(conn)) {
      conn.close()
    }
  }
}
