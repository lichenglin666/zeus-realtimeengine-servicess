package com.jindanfenqi.spark.stream

import com.jindanfenqi.spark.conf.HbaseConfig
import com.jindanfenqi.spark.model.{ChannelModel, UserIndicatorModel}
import com.jindanfenqi.spark.sources.HbaseSources
import com.jindanfenqi.spark.utils.JdbcUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * 计算用户指标任务
  *
  * 单线程，周期执行
  */
class CalcUserIndicatorTask(@transient val spark: SparkSession, tableName: String)
  extends Runnable with Serializable with Logging {
  import spark.implicits._

  /**
    * 用户渠道静态表，需要关联渠道类型
    */
  val channel = spark.sql(
    "select cast(id as long) as channelId, typename as channelType from dim.dim_biz_channel"
  ).cache().as[ChannelModel]

  override def run(): Unit = {
    logInfo(s"CalcUserIndicatorTask starts running}")

    /**
      * 计算最终的聚合结果, 从 HBase 中加载数据
      */
    val hbaseTable = HbaseSources.load(spark, HbaseConfig.toHbaseConfig(tableName))
    val result = UserIndicatorStreamCompute.calcUserIndicatorResult(spark, hbaseTable.as[UserIndicatorModel], channel)
    JdbcUtils.save(result)
  }
}
