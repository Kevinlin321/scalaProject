package cn.yongjie.scalaLanguageTest

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import javax.ws.rs.ext.RuntimeDelegate

object SQLTest {
  def main(args: Array[String]): Unit = {

    val url = "jdbc:mysql://localhost:3306/world"
    val user = "root"
    val password = "mysql"

    var connection: Connection = null
    var statement: Statement = null
    var resultSet: ResultSet = null

//    val jdbcOptions = new JDBCOptions(Map(
//      JDBCOptions.JDBC_URL -> url,
//      "user" -> user,
//      "password" -> password,
//      JDBCOptions.JDBC_DRIVER_CLASS -> "com.mysql.jdbc.Driver"
//    ))

    /*
    JDBC
     */

//    try{
//      classOf[com.mysql.jdbc.Driver]
//
//      connection = DriverManager.getConnection(url, user, password)
//      statement = connection.createStatement()
//      resultSet = statement.executeQuery("select * from city limit 10")
//      while(resultSet.next()){
//        val name = resultSet.getString("Name")
//        println(name)
//      }
//    } catch {
//      case e: Exception => println(e.printStackTrace())
//    } finally {
//      connection.close()
//    }

    /*
    SPARKSQL  connect to sql
     */

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("sqlTest")
      .getOrCreate()

    val sqlContext: SQLContext = spark.sqlContext

    val url2 = "jdbc:mysql://localhost:3306/mytest"
    val table = "test"
    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", password)
    prop.put("driver", "com.mysql.jdbc.Driver")

    val df = sqlContext
      .read
      .jdbc(url2, table, prop)

    df.show()


  }
}
