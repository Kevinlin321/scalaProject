package cn.yongjie.redisTest

import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}
import redis.clients.util.Pool

object RedisTest{

  private[this] var jedisPool: Pool[Jedis] = _

  def main(args: Array[String]): Unit = {

    val password = ""
    val host = ""
    val port = 6379
    val timeout = 1000
  }


  def init(host: String, port: Int, timeout: Int, password: String, database: Int = 0) = {

    jedisPool = new JedisPool(new GenericObjectPoolConfig, host, port, timeout, password, database)
  }

  // get value through key
  def get(key: Array[Byte]): Array[Byte] = {

    val jedis = jedisPool.getResource
    val result: Array[Byte] = jedis.get(key)
    jedis.close()
    result
  }

  def set(key: Array[Byte], value: Array[Byte]): Boolean = {

    try{
      val jedis = jedisPool.getResource
      jedis.set(key, value)
      jedis.close()
      true
    } catch {
      case ex: Exception => ex.printStackTrace()
        false
    }
  }


  def getCols(key: String,
             cols: Array[String] = Array.empty
             ): Map[String, Array[Byte]] = {

    import scala.collection.JavaConverters._
    val jedis = jedisPool.getResource
    var map = Map.empty[String, Array[Byte]]
    if (cols.length > 0) {
      val pipe = jedis.pipelined()
      val response = pipe.hmget(key.getBytes, cols.map(_.getBytes()): _*)
      pipe.sync()
      map = cols.zip(response.get.asScala).toMap.filter(x => x._2 != null)
      pipe.close()
    } else {
      val tmpMap: util.Map[Array[Byte], Array[Byte]] = jedis.hgetAll(key.getBytes())
      map = tmpMap.asScala.toMap.map(x => (new String(x._1), x._2))
    }
    jedis.close()
    map
  }

  def getCols2(
                key: String,
                cols: Array[String] = Array.empty
              ): Map[String, Array[Byte]] = {
    val jedis = jedisPool.getResource
    var map = Map.empty[String, Array[Byte]]
    if (cols.length > 0) {
      for (col <- cols) {
        val value: Array[Byte] = jedis.hget(key.getBytes(), col.getBytes())
        if (null != value) {
          map = map + (col -> value)
        }
      }
    } else {
      val tmpMap: util.Map[Array[Byte], Array[Byte]] = jedis.hgetAll(key.getBytes())
      import scala.collection.JavaConverters._
      map = tmpMap.asScala.toMap.map(x => (new String(x._1), x._2))
    }
    jedis.close
    map
  }

  def setCols(
             key: String,
             fielsValues: Map[String, String]
             ): Unit = {
    import scala.collection.JavaConverters._
    val data = fielsValues.map(element =>{
      (element._1.getBytes(), element._2.getBytes())
    }).asJava

    val jedis = jedisPool.getResource
    jedis.hmset(key.getBytes(), data)
    jedis.close()
  }


}
