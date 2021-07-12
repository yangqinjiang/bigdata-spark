package offline.test

import offline.utils.IpUtils
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
 * 测试使用ip2Region工具库解析IP地址为省份和城市
 */
object ConvertIpTest {

  def main(args: Array[String]): Unit = {
    val ip = "223.73.225.16"
    // a. 创建DbSearch对象，传递字典文件
    val dbSearcher = new DbSearcher(new DbConfig(), "dataset/ip2region.db")
    // b. 依据IP地址解析
    //val dataBlock: DataBlock = dbSearcher.btreeSearch("223.73.225.16")
//    val dataBlock: DataBlock = dbSearcher.btreeSearch("223.73.225.16")
    // 中国|0|海南省|海口市|教育网
//    val region: String = dataBlock.getRegion
//    println(s"$region")
    // c. 分割字符串，获取省份和城市
//    val Array(_, _, province, city, _) = region.split("\\|")
//    println(s"省份 = $province, 城市 = $city")

    val region = IpUtils.convertIpToRegion(ip, dbSearcher)
    println(s"省份 = ${region.province}, 城市 = ${region.city}")
  }
}
