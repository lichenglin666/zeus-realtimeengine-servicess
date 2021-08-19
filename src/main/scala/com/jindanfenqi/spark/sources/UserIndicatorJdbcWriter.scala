package com.jindanfenqi.spark.sources

import java.sql.{Connection, DriverManager, Statement}

import com.jindanfenqi.spark.model.MySqlUserIndicatorModel
import org.apache.spark.sql.ForeachWriter


class UserIndicatorJdbcWriter(url: String, table: String, user: String, password: String) extends ForeachWriter[MySqlUserIndicatorModel] {
  val driverClassName = "com.mysql.jdbc.Driver"
  var conn: Connection = _
  var statement: Statement = _

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driverClassName)
    conn = DriverManager.getConnection(url, user, password)
    statement = conn.createStatement()

    true
  }

  override def process(value: MySqlUserIndicatorModel): Unit = {
    val sql = s"insert into $table (ts, channelType, eventType, quantity) values ('${value.ts}', '${value.channelType}', '${value.eventType}', ${value.quantity})"
    statement.executeUpdate(sql)
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (!conn.isClosed) {
      conn.close()
    }
  }
}
