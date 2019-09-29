package cn.yongjie.sparkTest

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{callUDF, col}

object UDFRegisterTest {
  def main(args: Array[String]): Unit = {

    val getDay: String => Int = (date: String) => {

      val time = LocalDateTime.parse(date,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
      time.getDayOfMonth
    }

    val getMonth: String => Int = (date: String) => {
      val time = LocalDateTime.parse(date,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))

      time.getMonthValue
    }

    val getYear: String => Int = (date: String) => {
      val time = LocalDateTime.parse(date,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))

      time.getYear
    }

    val getHour:String => Int = (data:String) =>{
      val time = LocalDateTime.parse(data,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))

      time.getHour
    }

    var map: Map[String, Function[String, Int]] = Map[String, Function[String, Int]]()

    map += ("getDay" -> getDay)
    map += ("getMonth" -> getMonth)
    map += ("getYear" -> getYear)
    map += ("getHour" -> getHour)

    callUDF("getDay", col("123"))

  }

  def register(map: Map[String, Function[String, Int]], sqlSc: SQLContext): Unit = {

    map.keySet.foreach(f => {
      val value = map(f)
      value match{
        case `value` if value.isInstanceOf[Function[String, Int]] =>
          sqlSc.udf.register(f, value.asInstanceOf[Function[String, Int]])
      }
    })
  }

}
