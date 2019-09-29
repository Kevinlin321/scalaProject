package cn.yongjie.test

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import cn.yongjie.boschDataParse.AppConfig
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col, to_date, to_timestamp}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, TimestampType}

object RepartitionTest {



  def main(args: Array[String]): Unit = {

    val getDay: String => Int = (date: String) => {

      val time = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      time.getDayOfMonth
    }

    val getMonth: String => Int = (date: String) => {
      val time = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      time.getMonthValue
    }

    val getYear: String => Int = (date: String) => {
      val time = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      time.getYear
    }

    val getHour: String => Int = (data: String) => {
      val time = LocalDateTime.parse(data, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      time.getHour
    }



    var map: Map[String, Function[String, Int]] = Map[String, Function[String, Int]]()

    map += ("getDay" -> getDay)
    map += ("getMonth" -> getMonth)
    map += ("getYear" -> getYear)
    map += ("getHour" -> getHour)

    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", "200")
      .master("local[*]")
      .appName("BoschAnalysis")
      .getOrCreate()

    val df: DataFrame = spark
      .read
      .option("mergeSchema", "true")
      .option("basePath", AppConfig.getInputPath)
      .parquet(AppConfig.getInputPath)
      //.where("ts is not null")
      .select(
      col("vin"),
      col("ts").cast(LongType).as("ts"),
      col("timestamp").cast(TimestampType).as("timestamp"),
      col("lat").cast(DoubleType).as("lat"),
      col("lon").cast(DoubleType).as("lon"))

    register(map, df.sqlContext)

    df.createOrReplaceTempView("tb")

    spark.sql(
      """
        |SELECT
        | DISTINCT(HOUR(timestamp)) AS hour
        |FROM
        | tb
      """.stripMargin).show()

//    df
//      .withColumn("timestamp2", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"))
//      .withColumn("year", callUDF("getYear", col("timestamp2")))
//      .withColumn("month", callUDF("getMonth", col("timestamp2")))
//      .withColumn("day", callUDF("getDay", col("timestamp2")))
//      .withColumn("hour",callUDF("getHour",col("timestamp2")))
//      .repartition(col("year"), col("month"), col("day"), col("hour"))
//      .write
//      .partitionBy("year", "month", "day", "hour")
//      .mode(SaveMode.Overwrite)
//      .format("parquet")
//      .save("outTest/out2/")

  }

  def register(map: Map[String, Function[String, Int]], sqlSc: SQLContext): Unit = {

    map.keySet.foreach(f => {
      val value = map(f)
      value match {
        case `value` if value.isInstanceOf[Function[String, Int]] =>
          sqlSc.udf.register(f, value.asInstanceOf[Function[String, Int]])
      }
    })
  }
}
