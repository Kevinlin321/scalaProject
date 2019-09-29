package cn.yongjie.sparkTest

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object StructTypeTest {

  def main(args: Array[String]): Unit = {

    val schema: StructType = StructType(
      List(
        StructField("vin", StringType, nullable = true),
        StructField("lat", LongType, nullable = true)
      )
    )


  }
}
