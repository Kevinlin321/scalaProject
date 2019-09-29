package cn.yongjie.utilsTest

import java.util.Properties

object PropertyTest {
  def main(args: Array[String]): Unit = {

      val properties = new Properties()
      properties.put("1", "111")
      properties.put("2", "222")
      println(properties)

  }

}
