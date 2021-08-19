package com.jindanfenqi.spark.model

/**
  * 用户渠道, hive 表 dim.dim_biz_channel
  *
  * @param channelId  渠道ID
  * @param channelType 渠道类型
  */
case class ChannelModel(channelId: Long, channelType: String)
