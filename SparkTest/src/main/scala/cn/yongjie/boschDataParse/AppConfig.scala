package cn.yongjie.boschDataParse

import com.typesafe.config.ConfigFactory

object AppConfig {

  private val config = ConfigFactory.load("application.conf")

  val appName: String = config.getString("spark.appName")

  val baseInputPath: String = config.getString("spark.baseInputPath")

  val baseOutputPath: String = config.getString("spark.baseOutputPath")

  val ScriptPath: String = config.getString("spark.scriptPath")

  val intakeTemperatureOutName: String = config.getString("spark.intakeTemperatureOutName")

  val fuelConsumptionName: String = config.getString("spark.fuelConsumptionName")

  val speedName: String = config.getString("spark.speedName")

  val accelerateName: String = config.getString("spark.accelerateName")

  val speedAccName: String = config.getString("spark.speedAccName")

  val driveDistancePerDay: String = config.getString("spark.driveDistancePerDay")

  val driveDistancePerTrip: String = config.getString("spark.driveDistancePerTrip")

  val driveTimePerTrip: String = config.getString("spark.driveTimePerTrip")

  val engineSpeed: String = config.getString("spark.engineSpeed")

  val gearBox: String = config.getString("spark.gearBox")

  val day: String = config.getString("spark.day")

  val parquet: String = config.getString("spark.parquet")

  val csv: String = config.getString("spark.csv")

  def getInputPath: String = {
    baseInputPath + day
  }

  def getTemperatureOutPath: String = {
    baseOutputPath + day + intakeTemperatureOutName
  }

  def getFuelConsumptionOutPath: String = {
    baseOutputPath + day + fuelConsumptionName
  }

  def getSpeedOutPath: String = {
    baseOutputPath + day + speedName
  }

  def getAccOutPath: String = {
    baseOutputPath + day + accelerateName
  }

  def getDistancePerDayOutPath: String = {
    baseOutputPath + day + driveDistancePerDay
  }

  def getDistancePerTripOutPath: String = {
    baseOutputPath + day + driveDistancePerTrip
  }

  def getTimePerTripOutPath: String = {
    baseOutputPath + day + driveTimePerTrip
  }

  def getEngineSpeedOutPath: String = {
    baseOutputPath + day + engineSpeed
  }

  def getGearBoxOutPath: String = {
    baseOutputPath + day + gearBox
  }

  def getSpeedAccOutPath: String = {
    baseOutputPath + day + speedAccName
  }

}
