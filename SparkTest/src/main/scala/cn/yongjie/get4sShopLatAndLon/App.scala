package cn.yongjie.get4sShopLatAndLon

import java.io._
import java.sql.Connection

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer
import scalaj.http.{Http, HttpRequest}
import scala.io.Source
import scala.collection.mutable

object App {

  val logger: Logger = Logger.getLogger(App.getClass)
  val car4sset: mutable.Set[String] = mutable.Set[String]()
  val carNo4sSet: mutable.Set[String] = mutable.Set[String]()

  def main(args: Array[String]): Unit = {

    // get all 4s city code
    val citycodeList = get4SCitycode()
    // get mysql connection object
    val conn = MysqlMyUtils.getConnection(AppConfig.url, AppConfig.user, AppConfig.password)

    //----------------------------------------step 1: insert all 4s info----------------------------------------------
    // get exists id set in database
    val id4sSet: mutable.Set[String] = MysqlMyUtils.get4sStoreId(conn, 1)
    // process 4s info
    citycodeList.foreach(citycode => {
      logger.info(s"Processing 4s info for citycode=$citycode")
      println(s"Processing 4s info for citycode=$citycode")
      var page = 1
      val requestUrl = UrlUitls.getRequest4SGPSUrlThroughKeywords(city = citycode, page = page.toString)
      val count = get4SGPSAndName(requestUrl, conn, id4sSet)
      var restCount = count - 20
      while (restCount > 0) {
        page = page + 1
        restCount = restCount - 20
        get4SGPSAndName(UrlUitls.getRequest4SGPSUrlThroughKeywords(city = citycode, page = page.toString), conn, id4sSet)
      }
    })

    //---------------------------------------step 2: insert all car repair store info---------------------------------
    //    val id4sSet_2 = MysqlMyUtils.get4sStoreId(conn, 1)
    //    val idNo4sSet = MysqlMyUtils.get4sStoreId(conn, 0)
    //
    //    citycodeList.foreach(citycode => {
    //      logger.info(s"Processing no 4s info for citycode=$citycode")
    //      var page = 1
    //      val requestUrl = UrlUitls.getRequest4SGPSUrlThroughTypes(city = citycode, page = page.toString, types = "030000")
    //      val count = getcarRepairInfo(requestUrl, conn, id4sSet_2, idNo4sSet)
    //      var restCount = count - 20
    //      while (restCount > 0) {
    //        page = page + 1
    //        restCount = restCount - 20
    //        getcarRepairInfo(UrlUitls.getRequest4SGPSUrlThroughTypes(city = citycode, page = page.toString, types = "030000"), conn, id4sSet_2, idNo4sSet)
    //      }
    //    }

    MysqlMyUtils.close(conn)
  }

  def getcarRepairInfo(requestUrl: String, conn: Connection, id4sSet: mutable.Set[String], idNo4sSet: mutable.Set[String]): Int = {

    var count = 0 // get result info count
    try {
      val request4sGPS: HttpRequest = Http(requestUrl)
      val response4sGPS: String = request4sGPS.asString.body

      val jsonObjRes = JSON.parseObject(response4sGPS)
      val status = jsonObjRes.getInteger("status")
      if (status == 0) {
        //        println("get 4s gps info failed")
        //        println(requestUrl)
        logger.error("Get 4s gps info failed")
        logger.error(requestUrl)
      }
      count = jsonObjRes.getInteger("count")
      if (count > 1000) {
        //        println("return count larger than 1000")
        //        println(requestUrl)
        logger.error("Return count larger than 1000")
        logger.error(requestUrl)
      }

      val pois = jsonObjRes.getJSONArray("pois")
      val poiIter = pois.iterator()
      while (poiIter.hasNext()) {

        val poi = poiIter.next().asInstanceOf[JSONObject]
        val id = poi.getString("id")
        if (!id4sSet.contains(id) && !idNo4sSet.contains(id) && !carNo4sSet.contains(id)) {
          carNo4sSet += id
          val name = poi.getString("name")
          val location = poi.getString("location")
          val gps = location.split(",")
          val lon = gps(0).toDouble
          val lat = gps(1).toDouble
          val address = poi.getString("address")
          val tel = poi.getString("tel")
          //          println(s"id:$id;name:$name;lon:$lon;lat:$lat;address:$address;tel:$tel")
          logger.info(s"Increase--id:$id;name:$name;lon:$lon;lat:$lat;address:$address;tel:$tel")
          // creater carRepairInfo object
          val carRepair = CarRepairInfo(id, name, lon, lat, false, address, tel)
          MysqlMyUtils.insertIntoMySQL(conn, carRepair)
        }
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        //        val logOutPath = "D:\\Program\\github\\scalaProject\\SparkTest\\homeData\\4sInfo\\log.txt"
        //        val writer = new PrintWriter(new FileOutputStream(logOutPath, true))
        //        writer.write(requestUrl)
        //        writer.close()
        logger.error(ex.toString)
        logger.error(requestUrl)
      }
    }
    count
  }

  // get 4s shop lat and lon in China
  // return count
  def get4SGPSAndName(requestUrl: String, conn: Connection, id4sSet: mutable.Set[String]): Int = {

    var count = 0 // get result info count
    try {
      val request4sGPS: HttpRequest = Http(requestUrl)
      val response4sGPS: String = request4sGPS.asString.body

      val jsonObjRes = JSON.parseObject(response4sGPS)
      val status = jsonObjRes.getInteger("status")
      if (status == 0) {
        //        println("get 4s gps info failed")
        //        println(requestUrl)
        logger.error("Get 4s gps info failed")
        logger.error(requestUrl)
      }
      count = jsonObjRes.getInteger("count")
      if (count > 1000) {
        //        println("return count larger than 1000")
        //        println(requestUrl)
        logger.error("Return count larger than 1000")
        logger.error(requestUrl)
      }

      val pois = jsonObjRes.getJSONArray("pois")
      val poiIter = pois.iterator()
      while (poiIter.hasNext()) {

        val poi = poiIter.next().asInstanceOf[JSONObject]
        val id = poi.getString("id")
        if (!car4sset.contains(id) && !id4sSet.contains(id)) {
          car4sset += id
          val name = poi.getString("name")
          val location = poi.getString("location")
          val gps = location.split(",")
          val lon = gps(0).toDouble
          val lat = gps(1).toDouble
          val address = poi.getString("address")
          val tel = poi.getString("tel")
          //          println(s"id:$id;name:$name;lon:$lon;lat:$lat;address:$address;tel:$tel")
          logger.info(s"Increase--id:$id;name:$name;lon:$lon;lat:$lat;address:$address;tel:$tel")
          // creater carRepairInfo object
          val carRepair = CarRepairInfo(id, name, lon, lat, true, address, tel)
          MysqlMyUtils.insertIntoMySQL(conn, carRepair)
        }
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        //        val logOutPath = "D:\\Program\\github\\scalaProject\\SparkTest\\homeData\\4sInfo\\log2.txt"
        //        val writer = new PrintWriter(new FileOutputStream(logOutPath, true))
        //        writer.write(requestUrl)
        //        writer.close()
        logger.error(ex.toString)
        logger.error(requestUrl)
      }
    }
    count
  }

  // 通过 “4S店”的关键词和城市码“100000”中华人民共和国 找到所有存在4s店的城市码
  // 获取所有的城市码
  def get4SCitycode(): ListBuffer[String] = {

    val citycodeOutPath = "D:\\Program\\github\\scalaProject\\SparkTest\\homeData\\4sInfo\\citycode.txt"
    val citycodeFile = new File(citycodeOutPath)
    val citycodeList = new ListBuffer[String]()

    // judge whether file is exist
    if (citycodeFile.exists()) {

      val source = Source.fromFile(citycodeFile, "UTF-8")
      val resList = source.getLines().toList
      resList.foreach(x => {
        citycodeList += x
      })
      citycodeList
    } else {

      val requestUrl = UrlUitls.getRequest4SGPSUrlThroughKeywords(city = "100000", page = "1")
      val request4sGPS: HttpRequest = Http(requestUrl)
      val response4sGPS: String = request4sGPS.asString.body
      val jsonObjRes = JSON.parseObject(response4sGPS)
      val cities = jsonObjRes.getJSONObject("suggestion").getJSONArray("cities")
      val cityIter = cities.iterator()
      while (cityIter.hasNext()) {
        val city: JSONObject = cityIter.next().asInstanceOf[JSONObject]
        val adcode = city.getString("adcode")
        citycodeList += adcode
      }
      val writer = new PrintWriter(new File(citycodeOutPath))
      citycodeList.foreach(x => {
        writer.write(x + "\n")
      })
      writer.close()
      citycodeList
    }
  }

}
