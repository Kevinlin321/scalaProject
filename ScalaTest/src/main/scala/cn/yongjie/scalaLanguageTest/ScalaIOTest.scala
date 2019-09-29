package cn.yongjie.scalaLanguageTest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.io.Source

object ScalaIOTest {
  def main(args: Array[String]): Unit = {

    /*
    check whether the path is in hdfs path
     */
    val conf = new Configuration()
    val path = new Path("hadoopPath")
    val fs = path.getFileSystem(conf)
    fs.exists(path)

  }
}
