package cn.yongjie.moiverecom.online

import java.util

import cn.yongjie.moiverecom.common.{AppConfig, MyKafkaUtils, RedisUtils}
import cn.yongjie.moiverecom.model.ClickEventSim
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.ArrayBuffer

object RealTimeRecommend {

  def sendSimClickEventToKafka(): Unit = {

    var clickList: List[ClickEventSim] = Nil

    val click01 = ClickEventSim(1233, 456)
    val click02 = ClickEventSim(109, 232)
    val click03 = ClickEventSim(374, 272)

    clickList = clickList :+ click01 :+ click02 :+ click03
    println(clickList)

    for (click <- clickList) {
      println(click)
      MyKafkaUtils.sent(JSON.toJSONString(click, SerializerFeature.PrettyFormat))
    }

    MyKafkaUtils.close()
  }


  def createContext(): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieRecom")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "8000")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.locality.wait", "100")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //    val kafkaParams = scala.collection.mutable.Map(
    //      "bootstrap.servers" -> AppConfig.kafkaBroker,
    //      "key.deserializer" -> classOf[StringDeserializer],
    //      "value.deserializer" -> classOf[StringDeserializer],
    //      "auto.offset.reset" -> "latest",
    //      "group.id" -> AppConfig.kafkaGroup,
    //      "enable.auto.commit" -> (false: java.lang.Boolean))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> AppConfig.kafkaBroker,
      "auto.offset.reset" -> "smallest",
      "enable.auto.commit" -> "false",
      "group.id" -> AppConfig.kafkaGroup
    )

    val topicSet: Set[String] = AppConfig.kafkaTopic.split(",").toSet

    val message: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topicSet)

//    message.map(_._2)
//      .map(event => JSON.parseObject(event, classOf[ClickEventSim]))
//      .foreachRDD(rdd => {
//      rdd.foreach(println)
//    })


    import scala.collection.JavaConversions._

    message.map(_._2).map{event =>
      JSON.parseObject(event, classOf[ClickEventSim])
    }.mapPartitions{ iter =>{

      RedisUtils.init(AppConfig.redisHost,
        AppConfig.redisPort,
        AppConfig.redisTimeout,
        AppConfig.redisPassword,
        AppConfig.redisDatabase)
      val jedis = RedisUtils.getJedis
      iter.map{event =>{

        val userId = event.userId
        val movieId = event.movieId
        val seenMovies: util.List[Integer] = JSON.parseArray(jedis.get("UI-" + userId), classOf[Integer])
        println(seenMovies)
        val simMovies: util.List[Integer] = JSON.parseArray(jedis.get("UU-" + movieId), classOf[Integer])
        println(simMovies)
        val recom = recommendTop10Movies(seenMovies, simMovies)
        println(recom)
        recom
      }}
    }
    }.print()

    ssc.start()
    ssc.awaitTermination()

  }


  // recommend costumer top similar movies which have not seen by costumer
  def recommendTop10Movies(seenMovies: util.List[Integer], simMovies: util.List[Integer]): Array[Integer] = {

    val res = new ArrayBuffer[Integer]()
    for (i <- 0 until simMovies.size()) {
      if (!seenMovies.contains(simMovies.get(i))) {
        res.append(simMovies.get(i))
      }
    }
    res.toArray
  }

}
