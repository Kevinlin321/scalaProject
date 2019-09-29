package cn.yongjie.utilsTest

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

object TimeStampTest {
  def main(args: Array[String]): Unit = {


    /*
    print date information of today
     */
    val today: LocalDate = LocalDateTime.now().toLocalDate
    println(today)

    /*
    LocalDateTime transform
     */
    val pattern: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val zoneId: ZoneId = ZoneId.of("UTC+8")
    def toTimestamp(timeStr: String): Long = {
      LocalDateTime.parse(timeStr, pattern).atZone(zoneId).toInstant.toEpochMilli
    }
    def toTimeStr(timestamp: Long): String = {
      LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).format(pattern)
    }
    val res = toTimeStr(1565949669096L)
    val res2 = toTimestamp("2019-08-16 18:01:09.096")


    val getDay: String => Int = (date: String) => {

      val time = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))

      time.getDayOfMonth
    }

    val day = getDay("2019-09-22 12:33:55.352")
    println(day)

  }
}
