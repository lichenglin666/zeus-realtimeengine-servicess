package com.jindanfenqi.spark.conf

import com.typesafe.config.ConfigRenderOptions
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object HbaseConfig {
  private val config = ConfigManager.config.getConfig("hbase")

  val CATALOG = "catalog"
  val NEW_TABLE = "newtable"
  val CHECKPOINT_LOCATION = "checkpointLocation"
  val INTERVAL = "interval"


  def toHbaseConfig(table: String = ""): Map[String, String] = {
    require(table.nonEmpty, "table name mustn't be empty")

    val tableConfig = config.getConfig(table)

    Map(
      HBaseTableCatalog.tableCatalog ->
        tableConfig.getConfig(CATALOG).root.render(ConfigRenderOptions.concise),
      HBaseTableCatalog.newTable -> tableConfig.getString(NEW_TABLE)
    )
  }

  def checkpointLocation(table: String = ""): String = {
    require(table.nonEmpty, "table name mustn't be empty")

    config.getConfig(table).getString(CHECKPOINT_LOCATION)
  }


  def interval(table: String = ""): String = {
    require(table.nonEmpty, "table name mustn't be empty")

    config.getConfig(table).getString(INTERVAL)
  }

}
