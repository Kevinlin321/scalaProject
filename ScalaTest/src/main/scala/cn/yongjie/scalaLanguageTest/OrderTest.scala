package cn.yongjie.scalaLanguageTest

object OrderTest {
  def main(args: Array[String]): Unit = {

    val p1 = new Person("rain", 24)
    val p2 = new Person("rain", 22)
    val p3 = new Person("Lily", 15)
    val list = List(p1, p2, p3)

    // implicit function
    implicit object PersonOrdering extends Ordering[Person] {
      override def compare(p1: Person, p2: Person): Int = {
        p1.name == p2.name match {
          case false => p1.name.compareTo(p2.name)
          case _ => p1.age - p2.age
        }
      }
    }

    //list.sorted.foreach(println)
    //list.sortBy[Person](t => t).foreach(println)
    list.sortWith { (p1, p2) => {
      p1.name == p2.name match {
        case false => p1.name.compareTo(p2.name) < 0
        case _ => p1.age - p2.age < 0
      }
    }
    }.foreach(println)


    val array = Array(
      ("a", 5, 1),
      ("c", 3, 1),
      ("b", 1, 3)
    )

    val sortArray = array.sortBy(r => (r._3, r._1))(Ordering.Tuple2(Ordering.Int, Ordering.String.reverse))

    sortArray.foreach(println)
  }

    case class Person(name: String, age: Int) {
      override def toString: String = {
        name + "," + age
      }
    }


}



