package cn.yongjie.scalaLanguageTest

import scala.collection.mutable.ArrayBuffer

object ArrayTest {
  def main(args: Array[String]): Unit = {

//    val array = 1 to 5 toArray
//
//    println(array.head)
//    println(array.last)


    val array2 = new ArrayBuffer[Int]()

    array2.append(2)
    array2.+=(3)

    array2.foreach(println)


  }
}
