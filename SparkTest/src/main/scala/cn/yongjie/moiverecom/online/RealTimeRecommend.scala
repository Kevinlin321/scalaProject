package cn.yongjie.moiverecom.online

import cn.yongjie.moiverecom.common.{AppConfig, MyKafkaUtils}
import cn.yongjie.moiverecom.model.ClickEventSim
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils

object RealTimeRecommend {

  def sendSimClickEventToKafka(): Unit = {

    var clickList: List[ClickEventSim] = null

    val click01 = ClickEventSim(123, 456)
    val click02 = ClickEventSim(109, 232)
    val click03 = ClickEventSim(374, 272)

    clickList = clickList :+ click01 :+ click02 :+ click03

    for (click <- clickList) {
      MyKafkaUtils.sent(JSON.toJSONString(click, SerializerFeature.PrettyFormat))
    }
  }


  def createContext(): StreamingContext = {

    val conf = new SparkConf()
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
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )

    val topicSet = AppConfig.kafkaTopic.toSet

    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topicSet
    )

  }


}
