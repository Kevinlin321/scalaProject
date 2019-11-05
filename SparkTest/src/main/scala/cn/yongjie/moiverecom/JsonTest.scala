package cn.yongjie.moiverecom

import cn.yongjie.moiverecom.model.ClickEventSim
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}

//case class test(name:String)
object JsonTest {
  def main(args: Array[String]): Unit = {

    val click = ClickEventSim(1, 2)
    val conf = new SerializeConfig(true)
    val jsonObj = JSON.toJSONString(click, conf, SerializerFeature.PrettyFormat)
    println(jsonObj)

//    val testObj = test("123")
//    val conf = new SerializeConfig(true)
//    val jsonObj = JSON.toJSONString(testObj, conf)
//    println(jsonObj)
  }
}
