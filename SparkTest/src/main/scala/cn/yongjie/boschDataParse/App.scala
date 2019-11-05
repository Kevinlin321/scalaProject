package cn.yongjie.boschDataParse

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, TimestampType}
import org.apache.spark.sql.functions.{bround, col, count, lag, lead, sum}
import org.apache.spark.sql.expressions.Window
//import org.slf4j.{Logger, LoggerFactory}
import scala.sys.process._
import scalaj.http._
import com.alibaba.fastjson.{JSON, JSONObject}


object App {

  //final val logger: Logger = LoggerFactory.getLogger(App.getClass)
  final val isParseData: Boolean = true

  def main(args: Array[String]): Unit = {



//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//
//    if (isParseData) {
//
//      val spark: SparkSession = SparkSession
//        .builder()
//        .config("spark.debug.maxToStringFields", "200")
//        .master("local[3]")
//        .appName("BoschAnalysis")
//        .getOrCreate()
//
////      val df: DataFrame = spark
////        .read
////        .option("mergeSchema", "true")
////        .option("basePath", AppConfig.getInputPath)
////        .parquet(AppConfig.getInputPath)
////        //.where("ts is not null")
////        .select(
////          col("vin"),
////          col("ts").cast(LongType).as("ts"),
////          col("timestamp").cast(TimestampType).as("timestamp"),
////          col("lat").cast(DoubleType).as("lat"),
////          col("lon").cast(DoubleType).as("lon"),
////          col("carSpeed").cast(DoubleType).as("carSpeed"),
////          col("intakeTemperatureSize").cast(DoubleType).as("intakeTemperatureSize"),
////          col("meterInstantaneousFuelConsumption").cast(DoubleType).as("meterInstantaneousFuelConsumption"),
////          col("drivingCycleMileage").cast(LongType)./(1000).as("drivingCycleMileage"),
////          col("drivingCycleFlag").cast(IntegerType).as("drivingCycleFlag"),
////          col("engineSpeed").cast(DoubleType).as("engineSpeed"),
////          col("gearBox").cast(IntegerType).as("gearBox"),
////          col("coolWaterTemp").cast(DoubleType).as("coolWaterTemp"))
//
//
////      verifyAccIntegrateEqualZero(df,spark)
////      calculateSpeedAndAcc(df)
////      getCoolWaterTemperature(df)
////      getCarSpeedAndGearBoxData(df, spark)
////      calcTemperature(df)
////      calcFuelConsumption(df)
////      calcSpeed(df)
////      calcAccelerate(df)
////      calcDrivingDistancePerDay(df, spark)
////      calcDrivingTimeAndDistancePerTrip(df, spark)
////      calEngineSpeed(df)
////      calGearBox(df)
//
//    }else{
//      plotFigure("speed")
//    }

  }

  def getCarSpeedAndGearBoxData(df: DataFrame, spark: SparkSession): Unit = {

    val dfSpeedGearBox = df
      .select("vin", "ts", "timestamp", "carSpeed", "gearBox")
      .where("carSpeed is not null or gearBox is not null")
      .sort("vin", "ts")
      .withColumn("gearBoxLead", lead("gearBox", 1, 0).over(Window.partitionBy("vin").orderBy("ts")))
      .select("vin", "timestamp", "carSpeed", "gearBoxLead")
      .where("carSpeed is not null")

//    dfSpeedGearBox
//      .coalesce(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .csv("D:\\ImpTask\\01.PySparkVichileBigDataParse\\04.ScalaProgram\\BAICDataParse\\temp\\speed\\")

  }


  // get cool water temperature
  def getCoolWaterTemperature(df: DataFrame): Unit = {

    val dfCoolWater = df
      .select("vin", "ts", "timestamp", "coolWaterTemp", "lat", "lon")
      .where("coolWaterTemp is not null or lat is not null")
      .sort("vin", "ts")

    dfCoolWater
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv("D:\\ImpTask\\01.PySparkVichileBigDataParse\\04.ScalaProgram\\BAICDataParse\\temp\\coolwater\\")
  }

  // get cool water temperature
  def getCoolWaterTemperature2(df: DataFrame): Unit = {

    val dfCoolWater = df
      .select("vin", "ts", "timestamp", "coolWaterTemp", "lat", "lon")
      .where("coolWaterTemp is not null or lat is not null")
      .sort("vin", "ts")

    dfCoolWater.rdd.map(x => {
      val vin: String = x.getAs("vin")
      val lat: Double = x.getAs("lat")
      val lon: Double = x.getAs("lon")
      (vin, (lat, lon, 1))
    }).reduceByKey((x1, x2) =>
      (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3)
    ).map(x =>
      (x._1, x._2._1 / x._2._3, x._2._2 / x._2._3)
    )

  }


  // verify Integrate  Acc  equal zero during each trip
  def verifyAccIntegrateEqualZero(df: DataFrame, spark: SparkSession): Unit = {

    val dfCyclePre = df
      .where(
      """
        |ts is not null and
        |drivingCycleFlag is not null and
        |drivingCycleFlag != 0
      """.stripMargin)
      .select("vin", "ts", "drivingCycleFlag")
      .sort("vin", "ts")
      .withColumn("ts_next",
        lag("ts", 1, 0).over(Window.partitionBy("vin").orderBy("ts")))
      .withColumn("ts_delta", col("ts") - col("ts_next"))
      .where("ts_delta > 300000").
      withColumn("ts_next", lead("ts_next", 1, 0).over(Window.partitionBy("vin").orderBy("ts")))
      .select(
        col("vin"),
        col("ts"),
        col("ts_next"))
    dfCyclePre.createOrReplaceTempView("tb_pre")

    val dfMax = df.select("vin", "ts")
    dfMax.createOrReplaceTempView("tb_ts")

    val dfMaxRes = spark.sql(
      """
        |SELECT vin, MAX(ts) as max_ts
        |FROM tb_ts
        |GROUP BY vin
      """.stripMargin)
    dfMaxRes.createOrReplaceTempView("tb_max_ts")

    val dfCycleModify = spark.sql(
      """
        |SELECT
        |tb_b.vin,
        |tb_b.ts,
        |(CASE WHEN tb_b.ts_next=0 THEN tb_a.max_ts ELSE tb_b.ts_next END) as ts_next
        |FROM tb_max_ts as tb_a
        |JOIN tb_pre as tb_b
        |ON tb_a.vin=tb_b.vin
      """.stripMargin)

    //dfCycleModify.coalesce(1).write.option("header", "true").option("savemode", "overwrite").csv("D:\\Program\\Scala\\MyScalaTest\\data\\drivingdata\\")

//    dfCycleModify.rdd.map(x =>{
//
//      val vin = x.getAs[String]("vin")
//      val startTs = x.getAs[Long]("ts")
//      val endTs = x.getAs[Long]("ts_next")
//
//      df.select("vin", "ts", "carSpeed")
//        .where(s"carSpeed < 250 and vin=$vin")
//        .where(s"ts>$startTs and ts<$endTs")
//        .withColumn("speed_next", lag("carSpeed", 1, 0).over(Window.partitionBy("vin").orderBy("ts")))
//        .withColumn("time_next", lag("ts", 1, 0).over(Window.partitionBy("vin").orderBy("ts")))
//        .withColumn("acc", (col("carSpeed") - col("speed_next"))./(col("ts") - col("time_next")).*(1000)./(3.6))
//        .where("acc is not null and acc >= -8 and acc <=5")
//        .select("acc")
//        .agg(sum("acc").divide(count("acc")).as("accExp"))
//
//    })


    // LNBSCCAH4KT103660
    val vin = "LNBSCCAH4KT103660"
    val start = 1565949669096L
    val end = 1565950168652L

    val dfAcc = df
      .select(
      "vin",
      "ts",
      "carSpeed")
      .where(s"carSpeed < 250 and vin='$vin'")
      .where(s"ts>$start and ts<$end")
      .withColumn("speed_next", lag("carSpeed", 1, 0).over(Window.partitionBy("vin").orderBy("ts")))
      .withColumn("time_next", lag("ts", 1, 0).over(Window.partitionBy("vin").orderBy("ts")))
      .withColumn("acc", (col("carSpeed") - col("speed_next"))./(col("ts") - col("time_next")).*(1000)./(3.6))
      .where("acc is not null and acc >= -8 and acc <=5")
      .select("acc")
      .agg(sum("acc").divide(count("acc")).as("accExp"))
      .show()

//    dfAcc.createOrReplaceTempView("df_acc")
//    spark.sql(
//      """
//        |SELECT
//        |SUM(acc)/COUNT(acc) as expectation
//        |FROM df_acc
//      """.stripMargin).show()

  }


  // calculate speed and acc 2D plot figure
  def calculateSpeedAndAcc(df: DataFrame): Unit = {

    df.select(
      "vin",
      "ts",
      "carSpeed").
      where("carSpeed < 250").
      withColumn("speed_next", lag("carSpeed", 1, 0).over(Window.partitionBy("vin").orderBy("ts"))).
      withColumn("time_next", lag("ts", 1, 0).over(Window.partitionBy("vin").orderBy("ts"))).
      withColumn("acc", (col("carSpeed") - col("speed_next"))./(col("ts") - col("time_next")).*(1000)./(3.6)).
      sort("vin").
      where("acc is not null and acc >= -8 and acc <=5").
      select("vin", "acc", "carSpeed").
      coalesce(1).
      write.
      mode(SaveMode.Overwrite).
      option("header", "true").
      csv(AppConfig.getSpeedAccOutPath)
  }


  //calculate temperature
  def calcTemperature(df: DataFrame): Unit = {

    val dfTemperature = df.
      where("intakeTemperatureSize is not null").
      select("intakeTemperatureSize")

    dfTemperature.
      coalesce(1).
      write.
      mode(SaveMode.Overwrite).
      parquet(AppConfig.getTemperatureOutPath)
  }


  //calculate fuel consumption
  def calcFuelConsumption(df: DataFrame): Unit = {

    val dfFuelConsumption = df.
      where("meterInstantaneousFuelConsumption is not null").
      select("meterInstantaneousFuelConsumption")

    dfFuelConsumption.
      coalesce(1).
      write.
      mode(SaveMode.Overwrite).
      parquet(AppConfig.getFuelConsumptionOutPath)
  }


  // calculate speed
  def calcSpeed(df: DataFrame): Unit = {

    df.select("carSpeed").
      where("carSpeed < 250").
      coalesce(1).
      write.
      mode(SaveMode.Overwrite).
      parquet(AppConfig.getSpeedOutPath)
  }


  // calculate accelerate
  def calcAccelerate(df: DataFrame): Unit = {

    df.select(
      "vin",
      "ts",
      "carSpeed").
      where("carSpeed < 250").
      withColumn("speed_next", lag("carSpeed", 1, 0).over(Window.partitionBy("vin").orderBy("ts"))).
      withColumn("time_next", lag("ts", 1, 0).over(Window.partitionBy("vin").orderBy("ts"))).
      withColumn("acc", (col("carSpeed") - col("speed_next"))./(col("ts") - col("time_next")).*(1000)./(3.6)).
      select("acc").
      where("acc is not null and acc >= -8 and acc <=5").
      coalesce(1).
      write.
      mode(SaveMode.Overwrite).
      parquet(AppConfig.getAccOutPath)
  }


  // calculate driving distance per day
  def calcDrivingDistancePerDay(df: DataFrame, spark: SparkSession): Unit = {

    df.select(
      "vin",
      "drivingCycleMileage").
      createOrReplaceTempView("tb_drivingMile")

    spark.sql(
      """
        |SELECT
        |(MAX(drivingCycleMileage) - MIN(drivingCycleMileage)) AS distance
        |FROM tb_drivingMile
        |GROUP BY vin
      """.stripMargin).
      coalesce(1).
      write.
      mode(SaveMode.Overwrite).
      parquet(AppConfig.getDistancePerDayOutPath)
  }


  // calculate driving cycle time and distance
  def calcDrivingTimeAndDistancePerTrip(df: DataFrame, spark: SparkSession): Unit = {

    val dfCyclePre = df.where(
      """
        |ts is not null and
        |(drivingCycleFlag is not null or drivingCycleMileage is not null)
      """.stripMargin).
      select("vin", "ts", "drivingCycleFlag", "drivingCycleMileage").
      withColumn("drivingCycleMileageLead",
        lead("drivingCycleMileage", 1, 0).over(Window.partitionBy("vin").orderBy("ts"))).
      sort("vin", "ts").
      where("drivingCycleMileageLead is not null and drivingCycleMileageLead != 0").
      where("drivingCycleFlag is not null and drivingCycleFlag != 0").
      withColumn("ts_next",
        lag("ts", 1, 0).over(Window.partitionBy("vin").orderBy("ts"))).
      withColumn("driving_next",
        lag("drivingCycleMileageLead", 1, 0).over(Window.partitionBy("vin").orderBy("ts"))).
      withColumn("ts_delta", col("ts") - col("ts_next")).
      withColumn("mile_delta", col("drivingCycleMileageLead") - col("driving_next")).
      where("ts_delta > 300000").
      withColumn("ts_next", lead("ts_next", 1, 0).over(Window.partitionBy("vin").orderBy("ts"))).
      withColumn("driving_next", lead("driving_next", 1, 0).over(Window.partitionBy("vin").orderBy("ts"))).
      select(
        col("vin"),
        col("ts"),
        col("ts_next"),
        col("drivingCycleMileageLead").cast(LongType).as("drivingCycleMileageLead"),
        col("driving_next").cast(LongType).as("driving_next"))

    val dfMax = df.select(
      col("vin"),
      col("ts").cast(LongType).as("ts"),
      col("drivingCycleMileage").cast(LongType).as("drivingCycleMileage"))

    dfMax.createOrReplaceTempView("tb_ts_distance")

    val dfMaxRes = spark.sql(
      """
        |SELECT vin, MAX(ts) as max_ts, MAX(drivingCycleMileage) as max_distance
        |FROM tb_ts_distance
        |GROUP BY vin
      """.stripMargin)

    dfMaxRes.createOrReplaceTempView("tb_max_ts_distance")
    dfCyclePre.createOrReplaceTempView("tb_pre")

    val dfCycleModify = spark.sql(
      """
        |SELECT
        |tb_b.vin,
        |tb_b.ts,
        |(CASE WHEN tb_b.ts_next=0 THEN tb_a.max_ts ELSE tb_b.ts_next END) as ts_next,
        |tb_b.drivingCycleMileageLead,
        |(CASE WHEN tb_b.driving_next=0 THEN tb_a.max_distance ELSE tb_b.driving_next END) as driving_next
        |FROM tb_max_ts_distance as tb_a
        |JOIN tb_pre as tb_b
        |ON tb_a.vin=tb_b.vin
      """.stripMargin)

    // calculate distance per trip
    val dfDistancePerTrip = dfCycleModify.
      withColumn("distance_delta", (col("driving_next") - col("drivingCycleMileageLead")).divide(1000)).
      select("distance_delta")

    dfDistancePerTrip.
      coalesce(1).
      write.
      mode(SaveMode.Overwrite).
      parquet(AppConfig.getDistancePerTripOutPath)


    //calculate time per trip
    val dfTimePerTrip = dfCycleModify.
      withColumn("ts_delta", (col("ts_next") - col("ts")).divide(60000)).
      select("ts_delta")

    dfTimePerTrip.
      coalesce(1).
      write.
      mode(SaveMode.Overwrite).
      parquet(AppConfig.getTimePerTripOutPath)
  }


  // calculate engine speed
  def calEngineSpeed(df: DataFrame): Unit = {
    df
      .select("engineSpeed")
      .where("engineSpeed is not null")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(AppConfig.getEngineSpeedOutPath)
  }


  // calculate gear box
  def calGearBox(df: DataFrame): Unit = {
    df
      .select("gearBox")
      .where("gearBox is not null")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(AppConfig.getGearBoxOutPath)
  }


  //parse output data with figures and pickle data
  def plotFigure(variable: String): Unit = {
    """
      |temperature; fuelConsume; speed; accelerate; driveDisPerDay; drivePerTrip; all
    """.stripMargin
    val scriptStr = "python " + AppConfig.ScriptPath + " " + variable
    val process = Runtime.getRuntime.exec(scriptStr)
    val res = process.waitFor()
    println(res)

  }



  //get city code from Gaode Map through gps location
  def getCityThroughGPS(df: DataFrame, spark: SparkSession): Unit = {

    val dfGPS = df.selectExpr("vin", "ROUND(lon, 6) as lon", "ROUND(lat, 6) as lat")
    val dfGPSModify = dfGPS.where("lat is not null").select("vin", "lon", "lat")
    dfGPSModify.createOrReplaceTempView("tb_gps")

    val resDf = spark.sql(
      """
        |SELECT vin,
        |AVG(lon) AS lon,
        |AVG(lat) AS lat
        |FROM tb_gps
        |GROUP BY vin
      """.stripMargin)

    val distinctNum = resDf.count()

    val searchCityThroughGPSBaseString = "https://restapi.amap.com/v3/geocode/regeo?&key=dbc1529a1066f76b1de1cfa985d90c51"
    val locationGPSInfoString = "&location=113.063166,27.81064"

    val requestCityUrl = searchCityThroughGPSBaseString + locationGPSInfoString
    val requestCity: HttpRequest = Http(requestCityUrl)
    val responseCity: String = requestCity.asString.body

    val jsonObjCity = JSON.parseObject(responseCity)
    val baseObjCity = jsonObjCity.getJSONObject("regeocode").getJSONObject("addressComponent")
    val province = baseObjCity.getString("province")
    val city = baseObjCity.getString("city")
    val adcode = baseObjCity.getString("adcode")


    // get intime temperature
    val searchTemperatureThroughCityCodeBaseString = "https://restapi.amap.com/v3/weather/weatherInfo?&key=dbc1529a1066f76b1de1cfa985d90c51"
    val citycodeString = "&city=430211"

    val requestTempUrl = searchTemperatureThroughCityCodeBaseString + citycodeString
    val responseTemp = Http(requestTempUrl).asString.body
    val jsonObjTemp = JSON.parseObject(responseTemp)
    val temperature = jsonObjTemp.getJSONArray("lives").getJSONObject(0).getString("temperature")

  }


  def test(dfCyclePre: DataFrame, spark: SparkSession): Unit = {

    //replace zero with max number in cyclePre dataframe
    val dfCycleModify = dfCyclePre.rdd.map(x => {
      val vin = x.getAs[String]("vin")
      val curTs = x.getAs[Long]("ts")
      val curDistance = x.getAs[Long]("drivingCycleMileageLead")
      var nextTs = x.getAs[Long]("ts_next")
      var nextDistance = x.getAs[Long]("driving_next")
      if (curTs == 0) {
        nextTs = spark.sql(
          s"""
             |SELECT max_ts
             |FROM tb_max_ts_distance
             |WHERE vin=$vin
              """.stripMargin).first().getAs[Long]("max_ts")

        nextDistance = spark.sql(
          s"""
             |SELECT max_distance
             |FROM tb_max_ts_distance
             |WHERE vin=$vin
              """.stripMargin).first().getAs[Long]("max_distance")
      }
      (vin, curTs, nextTs, curDistance, nextDistance)
    })
    println("---------" + dfCycleModify.count())
    dfCycleModify.foreach(println)
    //        .toDF("vin", "ts", "ts_next", "drivingCycleMileage", "driving_next")

  }


}
