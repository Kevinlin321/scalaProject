package cn.yongjie.moiverecom.test

import cn.yongjie.moiverecom.model.ClickEventSim
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

object FastJsonTest {

  def main(args: Array[String]): Unit = {

    val click01 = ClickEventSim(1233, 456)
    println(click01.toString)
    val resStr = JSON.toJSONString(click01.toString, SerializerFeature.PrettyFormat)
    println(resStr)
  }
}
