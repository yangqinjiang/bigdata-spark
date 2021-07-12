package offline.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat



/**
 * 日期时间工具类
 */
object DataUtils {

  def getTodayDate():String = {
    // 获取日期
    val nowDate = new Date()
    FastDateFormat.getInstance("yyyy-MM-dd").format(nowDate)

  }
  //获取昨日的日期,格式为 20190710
  def getYesterdayDate():String = {
    // a. 获取Calendar对象
    val calendar: Calendar = Calendar.getInstance()
    // b. 获取昨日日期
    calendar.add(Calendar.DATE,-1)
    //转换日期格式
    FastDateFormat.getInstance("yyyy-MM-dd").format(calendar)
  }
}
