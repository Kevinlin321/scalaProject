package cn.yongjie.moiverecom

import java.util.Properties

import cn.yongjie.moiverecom.common.AppConfig
import cn.yongjie.moiverecom.offline.ALSRecomend
import cn.yongjie.moiverecom.online.RealTimeRecommend
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.log4j.{Level, Logger}


object APP {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // batch handle
//    ALSRecomend.batchOperation()

    // kafka producer send data to topic
//    RealTimeRecommend.sendSimClickEventToKafka()

    // kafka spark streaming
    RealTimeRecommend.createContext()

  }
}
