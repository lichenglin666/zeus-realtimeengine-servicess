package com.jindanfenqi.spark.conf

object KafkaConfig {
  private val config = ConfigManager.config

  val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
  val SUBSCRIBE = "subscribe"


  /**
    * 获取 kafka 的配置
    *
    * @param topic 如果没有设置从配置文件中读取
    * @return
    */
  def topicConfig(topic: String = ""): Map[String, String] = {
    Map(
      KAFKA_BOOTSTRAP_SERVERS -> config.getString(KAFKA_BOOTSTRAP_SERVERS),
      SUBSCRIBE -> {
        if (topic.nonEmpty) topic else config.getString("kafka." + SUBSCRIBE)
      }
    )
  }

}
