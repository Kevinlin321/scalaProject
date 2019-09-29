package cn.yongjie.utilsTest

import com.alibaba.fastjson.{JSON, JSONObject}

object FastJsonTest {

  def main(args: Array[String]): Unit = {

    /*
    get json object
     */
    val json01 = new JSONObject()
    val json02 = new JSONObject()
    json01.put("1", "111")
    json01.put("2", json02)
    json02.put("3", "333")
    json02.put("4", "444")
    println(json01.toJSONString)


    /*
    parse json object
     */
    val str = """{"1":"222", "2":"333"}"""

    var jObj = JSON.parseObject(str)
    val str01 = jObj.getString("1")
    println(str01)

  }

}
