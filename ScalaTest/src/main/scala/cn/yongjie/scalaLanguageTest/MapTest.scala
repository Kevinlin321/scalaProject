package cn.yongjie.scalaLanguageTest

import scala.collection.mutable
import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ArrayBuffer

object MapTest {

  def main(args: Array[String]): Unit = {

    val stsMap: mutable.Map[String, String=>Iterator[String]] = {
      val map = mutable.Map[String, String=>Iterator[String]]()
      map.put("20190923", parseSTS)
      map
    }

    val str = """a b c d e"""

    val strings = stsMap("20190923").apply(str)
    //.get("1").isDefined   判断是否定义了函数
    strings.foreach(println) // a b c d e

  }

  def parseSTS(record: String):Iterator[String] = {

    val strList = record.split(" ")
    strList.toIterator
  }
}
