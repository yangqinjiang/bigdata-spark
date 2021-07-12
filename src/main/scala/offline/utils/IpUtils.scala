package offline.utils

import offline.etl.Region
import org.lionsoul.ip2region.DbSearcher

/**
 * IP地址解析工具类
 */
object IpUtils {

  def convertIpToRegion(ip:String,dbSearcher: DbSearcher):Region = {
    // a,依据IP地址解析
    val dataBlock = dbSearcher.btreeSearch(ip)
    // 中国|0|海南省|海口市|教育网
    // b. 分割字符串，获取省份和城市
    val Array(_,_,province,city,_) = dataBlock.getRegion.split("\\|")
    // c,返回Region对象
    Region(ip,province,city)
  }
}
