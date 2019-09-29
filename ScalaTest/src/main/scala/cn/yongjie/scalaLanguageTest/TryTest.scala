package cn.yongjie.scalaLanguageTest

import scala.util.Try

object TryTest {
  def main(args: Array[String]): Unit = {

    val array = Array(0, 1, 2)
    //val num = array(3) // exception
    val num2 = Try(array(3)).getOrElse(0)
    println(num2)

    val numTry = Try(array(3))
    if (numTry.isFailure){
      println("fail")
    }
  }
}
