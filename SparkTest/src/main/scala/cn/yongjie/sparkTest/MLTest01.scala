package cn.yongjie.sparkTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MLTest01 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MLTest")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    val data = sqlContext
      .read
      .format("libsvm")
      .load("data/sample_linear_regression_data.txt")

    data.show()

  }
}
