package cn.yongjie.moiverecom.common

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}
import redis.clients.util.Pool

object RedisUtils extends Serializable {

  private[this] var jedisPool: Pool[Jedis] = _

  def init(host: String, port: Int, timeout: Int, password: String, database: Int): Unit = {

    jedisPool = new JedisPool(new GenericObjectPoolConfig, host, port, timeout, password, database)
  }

  def getJedis: Jedis = {
    jedisPool.getResource
  }

  def closePool: Unit = {
    jedisPool.close()
  }

  // set (key, value)
  def set(key: String, value: String): Boolean = {

    try {
      val jedis = jedisPool.getResource
      jedis.set(key, value)
      true
    } catch {
      case ex: Exception => ex.printStackTrace()
        false
    }
  }

  // get value from key
  def get(key: String): String = {

    val jedis = jedisPool.getResource
    val value = jedis.get(key)
    value
  }


}
