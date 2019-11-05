package cn.yongjie.get4sShopLatAndLon

import com.typesafe.config.ConfigFactory

object AppConfig {

  private val config = ConfigFactory.load("get4sInfo.conf")

  val url: String = config.getString("mysql.url")
  val user: String = config.getString("mysql.user")
  val password: String = config.getString("mysql.password")
}
