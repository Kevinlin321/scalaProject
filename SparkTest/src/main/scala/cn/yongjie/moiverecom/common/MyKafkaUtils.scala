package cn.yongjie.moiverecom.common

import java.util.Properties
import java.util.concurrent.Future

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

object MyKafkaUtils {

  private var properties = new Properties()
  properties.put("bootstrap.servers", AppConfig.kafkaBroker)
  properties.put("group.id", AppConfig.kafkaGroup)
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("batch.size", "16384")
  properties.put("linger.ms", "1")

  private val producer = new KafkaProducer[String, String](properties)

  // sent message to kafka
  def sent(message: String): Future[RecordMetadata] = {

    val record = new ProducerRecord[String, String](AppConfig.kafkaTopic, message)
    producer.send(record, new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
        if (e == null) {
          println(recordMetadata.partition() + "--" + recordMetadata.offset())
        } else {
          e.printStackTrace()
        }
      }
    })
  }

  def sent(message: JSONObject): Future[RecordMetadata] = {

    val record = new ProducerRecord[String, String](AppConfig.kafkaTopic, message.toJSONString)
    producer.send(record, new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
        if (e == null) {
          println(recordMetadata.partition() + "--" + recordMetadata.offset())
        } else {
          e.printStackTrace()
        }
      }
    })
  }

  // close producer
  def close(): Unit = {
    producer.close()
  }

}
