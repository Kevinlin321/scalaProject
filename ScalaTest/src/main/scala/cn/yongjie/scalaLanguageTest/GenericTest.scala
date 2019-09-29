package cn.yongjie.scalaLanguageTest

object GenericTest {
  def main(args: Array[String]): Unit = {

    val gt = new GenericTest1[Boy]
    val gt2 = new GenericTest2
    val huangbo = new Boy("huangbo", 60)
    val xuzheng = new Boy("xuzheng", 66)
    val boy = gt.choose(huangbo, xuzheng)
    val boy2 = gt2.choose(huangbo, xuzheng)
    print(boy2.name)

  }

  /*
  下面的意思就是表示只要是Comparable就可以传递,下面是类上定义的泛型
   */
  class GenericTest1[T <: Comparable[T]] {

    def choose(one: T, two: T): T = {
      if (one.compareTo(two) > 0) one else two
    }
  }

  /*
  在方法上定义泛型
   */
  class GenericTest2 {

    def choose[T <: Comparable[T]](one: T, two: T) = {
      if (one.compareTo(two) > 0) one else two
    }

  }

  class Boy(val name: String, val age: Int) extends Comparable[Boy] {
    override def compareTo(o: Boy): Int = {
      this.age - o.age
    }
  }

}
