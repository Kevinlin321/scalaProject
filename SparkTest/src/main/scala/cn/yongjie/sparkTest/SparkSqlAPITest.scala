package cn.yongjie.sparkTest

import org.apache.spark.sql.SparkSession

object SparkSqlAPITest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("APITest")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true")
      .csv("data/titanic_dataset.csv")

    /*
    |survived|pclass|                name|   sex|   age|sibsp|parch|  ticket|    fare|
     */

    df.createOrReplaceTempView("tb_tanic")

    /*
    test whether the fields used in order by should be in select fields
    the results show that it is unnecessary
     */
//    val res = spark.sql(
//      """
//        |SELECT pclass
//        |FROM tb_tanic
//        |ORDER BY age
//      """.stripMargin)
//
//    res.show()


    /*
    test sparksql function lit
     */

    //df.withColumn("time", lit("20190923")).show()  // it is ok
    //df.withColumn("time", "20190923") you can use it

  }
}
