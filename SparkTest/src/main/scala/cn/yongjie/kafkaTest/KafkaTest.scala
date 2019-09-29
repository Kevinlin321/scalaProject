package cn.yongjie.kafkaTest

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaTest {
  def main(args: Array[String]): Unit = {


  }

  def testProducer: Unit = {

    val prop = new Properties()
    prop.put("bootstrap.servers", "brokers")
    prop.put("client.id", "clientId")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val produce = new KafkaProducer[String, String](prop)


    /*
    send directly
     */
    val record = new ProducerRecord[String, String]("topic", "testTopic")

    /*
    send
     */

  }
}
