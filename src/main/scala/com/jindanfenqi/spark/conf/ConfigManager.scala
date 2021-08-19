package com.jindanfenqi.spark.conf

import com.typesafe.config.{Config, ConfigFactory}

object ConfigManager {
  private val ZEUS_PROFILE_ACTIVE = "zeus.profile.active"
  private val configure = ConfigFactory.load

  def config: Config = {
   val profile = configure.getString(ZEUS_PROFILE_ACTIVE)
    configure.getConfig(profile)
  }

}
