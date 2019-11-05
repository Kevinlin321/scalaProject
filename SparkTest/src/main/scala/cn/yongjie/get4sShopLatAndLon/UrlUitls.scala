package cn.yongjie.get4sShopLatAndLon

object UrlUitls {

  val search4sGPSBaseString = "https://restapi.amap.com/v3/place/text?&key=dbc1529a1066f76b1de1cfa985d90c51"
  val keywords = "4s店"
  val children = "0"
  val offset = "20"
  val extensions = "base"

  // get request 4s gps url through keywords "4s店"
  def getRequest4SGPSUrlThroughKeywords(city: String, page: String): String = {

    val infoString = s"&keywords=$keywords&types=&city=$city&children=$children&offset=$offset&page=$page&extensions=$extensions"
    search4sGPSBaseString + infoString
  }

  // get request car repair url through types
  def getRequest4SGPSUrlThroughTypes(city: String, page: String, types: String): String = {

    val infoString = s"&keywords=&types=$types&city=$city&children=$children&offset=$offset&page=$page&extensions=$extensions"
    search4sGPSBaseString + infoString
  }

}
