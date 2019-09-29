package cn.yongjie.aliyunTest

import java.util

import com.alicloud.openservices.tablestore.model.{PrimaryKey, PrimaryKeyColumn, PrimaryKeyValue, RangeRowQueryCriteria}
import com.aliyun.openservices.tablestore.hadoop._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TableStoreTest {
  def main(args: Array[String]): Unit = {

    val PRIMARY_KEY_NAME_1 = "DATE"
    val PRIMARY_KEY_NAME_2 = "VIN"
    val PRIMARY_KEY_NAME_3 = "CARTYPE"

    val hadoopConf = new Configuration()

    val sc = new SparkContext()

    // 获取实例的访问权限
    TableStore.setCredential(hadoopConf,
      new Credential("accessKeyId", "accessKeySecret", "securityToken"))
    TableStore.setEndpoint(hadoopConf, new Endpoint("endPoint", "instanceName"))

    // 设置范围查询的格式
    TableStoreInputFormat.addCriteria(hadoopConf, fetchCriteria("tablename", "startTime", "endTime"))

    //根据设置的范围查找内容
    def fetchCriteria(tableName: String,startDate:String,endDate:String): RangeRowQueryCriteria = {
      val res = new RangeRowQueryCriteria(tableName)
      res.setMaxVersions(1)
      val lower = new util.ArrayList[PrimaryKeyColumn]()
      val upper = new util.ArrayList[PrimaryKeyColumn]()
      lower.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(startDate)))
      upper.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(endDate)))
      lower.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MIN))
      upper.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX))
      lower.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_3, PrimaryKeyValue.INF_MIN))
      upper.add(new PrimaryKeyColumn(PRIMARY_KEY_NAME_3, PrimaryKeyValue.INF_MAX))

      res.setInclusiveStartPrimaryKey(new PrimaryKey(lower))
      res.setExclusiveEndPrimaryKey(new PrimaryKey(upper))
      res
    }

    val strRDD: RDD[String] = sc.newAPIHadoopRDD(
      conf = hadoopConf,
      fClass = classOf[TableStoreInputFormat], //将tableStore做为输入流
      kClass = classOf[PrimaryKeyWritable], //返回的key
      vClass = classOf[RowWritable]) //返回的value
      .map(row => row._2.getRow.getLatestColumn("DATA").getValue.toString)
      .repartition(12)

  }
}
