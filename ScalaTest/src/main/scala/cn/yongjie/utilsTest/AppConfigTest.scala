package cn.yongjie.utilsTest

import com.typesafe.config.ConfigFactory

object AppConfigTest {
  def main(args: Array[String]): Unit = {

//    import scala.collection.JavaConversions._
//
//    val config = ConfigFactory.load("appConfigTest.conf")
//
//    val oemList = config.getStringList("app.oem").toArray[String]
//    def getCarTypeList(oem: String): Array[String] = {
//
//      config.getStringList(s"app.$oem").toArray[String]
//    }
//
//    println(oemList.mkString(","))
//    println(getCarTypeList(oemList(0)).mkString(","))


    /*
    app.conf 中存在 %s的format，通过string.format进行还原
     */

//    val ossPath = config.getString("oss.oss_path")
//
//    def getOssPath(ossPath: String, oem: String, carType: String):String = {
//
//      String.format(ossPath, oem, carType)
//    }
//
//    println(getOssPath(ossPath, "BAIC", "C53F"))

  }
}
