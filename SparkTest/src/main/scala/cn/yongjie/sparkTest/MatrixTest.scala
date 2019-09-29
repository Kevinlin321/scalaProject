package cn.yongjie.sparkTest

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MatrixTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("MatrixTest")
      .getOrCreate()

    val data = Array(
      Vectors.dense(4.0, 2.0, 3.0),
      Vectors.dense(5.0, 6.0, 1.0),
      Vectors.dense(2.0, 4.0, 7.0),
      Vectors.dense(3.0, 6.0, 5.0)
    )

    val dataRDD: RDD[Vector] = spark.sparkContext.parallelize(data)
    val matrix = new RowMatrix(dataRDD)

//    val stasticSummary: MultivariateStatisticalSummary =matrix.computeColumnSummaryStatistics()
//    println(stasticSummary.mean)  // 结果：3.5,4.5,4.0


    val covariance: Matrix = matrix.computeCovariance()
    println(covariance.rowIter.next()(0))



  }

}
