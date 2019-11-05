package cn.yongjie.get4sShopLatAndLon

import java.sql.{Connection, DriverManager}

import scala.collection.mutable

object MysqlMyUtils {

  // get connnection
  def getConnection(url: String, user: String, password: String): Connection = {

    DriverManager.getConnection(url, user, password)
  }

  // close connection
  def close(conn: Connection): Unit = {

    try {
      if (!conn.isClosed() || conn != null) {
        conn.close()
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  // get exist 4s or no4s id
  def get4sStoreId(conn: Connection, id: Int): mutable.Set[String] = {

    val idSet = mutable.Set[String]()
    try {
      val sql = s"SELECT id FROM carrepairinfo Where is4S=$id"
      val pstm = conn.prepareStatement(sql)
      val resultSet = pstm.executeQuery()
      while (resultSet.next()) {
        idSet += resultSet.getString("id")
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    idSet
  }

  // insert into mysql
  def insertIntoMySQL(conn: Connection, carRepInfo: CarRepairInfo): Unit = {

    try {
      val sql = new StringBuilder()
        .append("INSERT INTO carrepairinfo(id, name, lon, lat, is4S, address, tel)")
        .append(" VALUES(?,?,?,?,?,?,?)")
      val pstm = conn.prepareStatement(sql.toString())
      pstm.setObject(1, carRepInfo.id)
      pstm.setObject(2, carRepInfo.name)
      pstm.setObject(3, carRepInfo.lon)
      pstm.setObject(4, carRepInfo.lat)
      pstm.setObject(5, carRepInfo.is4S)
      pstm.setObject(6, carRepInfo.address)
      pstm.setObject(7, carRepInfo.tel)

      pstm.executeUpdate()
    } catch {
      case ex: Exception => {
        println("exist:" + carRepInfo.id)
      }
    }
  }

}
