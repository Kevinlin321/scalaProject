package cn.yongjie.scalaLanguageTest

object QuotationTest {
  def main(args: Array[String]): Unit = {

    val func: String => Int = (date: String) => {

      date.length
    }

    //println(`func`.toString())


    val `value` = "value"
    println(`value`)
  }
}
