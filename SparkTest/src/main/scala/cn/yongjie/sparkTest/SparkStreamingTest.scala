package cn.yongjie.sparkTest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object SparkStreamingTest {
  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate("checkpointPath", createContext)
    ssc.start()

    ssc.awaitTerminationOrTimeout(10000)
    ssc.stop(true, true)


  }

  def createContext(): StreamingContext = {

    val conf = new SparkConf().setAppName("appName")
    conf.set("spark.streaming.backpressure.enabled","true")
    conf.set("spark.streaming.kafka.maxRatePerPartition","num")
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("checkpointPath")

    val kafkaParam =
      scala.collection.mutable.Map(
        "bootstrap.servers" -> "broker address",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "latest",
        "group.id" -> "groupid",
        "enable.auto.commit" -> (false: java.lang.Boolean))


    // 监听的topic列表
    val topics = new ArrayBuffer[String]()

    //DangerLevelHander.handle(kStream)
    ssc
  }
}
