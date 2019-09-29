package cn.yongjie.utilsTest

object RegexTest {
  def main(args: Array[String]): Unit = {

    val str = """1::Toy Story (1995)::Animation|Children's|Comedy"""
    val list = str.split("::")
    list(2).split("\\|").foreach(println)
  }
}
