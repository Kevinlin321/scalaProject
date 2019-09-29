package cn.yongjie.boschDataParse

import cn.yongjie.boschDataParse.AppConfig
import scala.sys.process._  // important to import is jar package

object ScalaUsePythonScriptsTest {
  def main(args: Array[String]): Unit = {

      val scriptStr = "python " + AppConfig.ScriptPath + " " + AppConfig.intakeTemperatureOutName
      val process = Runtime.getRuntime.exec(scriptStr)
      val waitFor = process.waitFor()
      println(waitFor)

  }

}
