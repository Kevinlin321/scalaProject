package cn.yongjie.moiverecom.common


import com.typesafe.config.ConfigFactory


object AppConfig {

  private val config = ConfigFactory.load("appMovie.conf")

  val redisHost: String = config.getString("redis.host")
  val redisPort: Int = config.getInt("redis.port")
  val redisTimeout: Int  = config.getInt("redis.timeout")
  val redisPassword: String = config.getString("redis.password")
  val redisDatabase: Int  = config.getInt("redis.database")

  val kafkaTopic: String = config.getString("kafka.topic")
  val kafkaBroker: String = config.getString("kafka.brokerList")
  val kafkaGroup: String = config.getString("kafka.group")


  val dataResource: String = config.getString("dataSource.basePath")
  val moviesDataPath: String = dataResource + config.getString("dataSource.movies")
  val usersDataPath: String = dataResource + config.getString("dataSource.users")
  val ratingsDataPath: String = dataResource + config.getString("dataSource.ratings")

}
