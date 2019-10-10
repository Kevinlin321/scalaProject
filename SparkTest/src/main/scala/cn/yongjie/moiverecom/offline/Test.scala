package cn.yongjie.moiverecom.offline

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}

object Test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

    ALSRecomend.batchOperation();
  }
}
