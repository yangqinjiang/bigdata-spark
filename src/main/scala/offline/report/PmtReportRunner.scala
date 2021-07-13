package offline.report

import offline.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * 针对广告点击数据,依据需求进行报表开发,具体说明如下
 * - 各地域分布统计: region_stat_analysis
 * - 广告区域统计 : ads_region_analysis
 * - 广告APP统计: ads_app_analysis
 * - 广告设备统计:ads_device_analysis
 * - 广告网络类型统计: ads_network_analysis
 * - 广告运营商统计: ads_isp_analysis
 * - 广告渠道统计: ads_channel_analysis
 */
object PmtReportRunner {

  def main(args: Array[String]): Unit = {

    //设置spark应用程序运行的用户root, 默认情况下为当前系统用户
//    System.setProperty("user.name", "root")
//    System.setProperty("HADOOP_USER_NAME", "root")

    //1 创建SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    //2 从Hive表中加载广告ETL数据
    val pmtDF: DataFrame = spark.read.table("itcast_ads.pmt_ads_info").where($"date_str" === date_sub(current_date(), 1))

    pmtDF.printSchema()
    pmtDF.select("uuid", "ip", "province", "city").show(20, truncate = false)

    //由于多张报表的开发,使用相同的数据,所以缓存
    pmtDF.persist(StorageLevel.MEMORY_AND_DISK)

    // 3依据不同业务需求开发报表
    /**
     * 不同业务报表统计分析时,两步骤
     * i,编写SQL或者DSL分析
     * ii,将分析结果保存MySQL数据库表中
     */

    //3.1 各地域分布统计: region_stat_analysis
    RegionStateReport.doReport(pmtDF)
    // 3.2. 广告区域统计：ads_region_analysis
    //AdsRegionAnalysisReport.doReport(pmtDF)
    // 3.3. 广告APP统计：ads_app_analysis
    //AdsAppAnalysisReport.processData(pmtDF)
    // 3.4. 广告设备统计：ads_device_analysis
    //AdsDeviceAnalysisReport.processData(pmtDF)
    // 3.5. 广告网络类型统计：ads_network_analysis
    //AdsNetworkAnalysisReport.processData(pmtDF)
    // 3.6. 广告运营商统计：ads_isp_analysis
    //AdsIspAnalysisReport.processData(pmtDF)
    // 3.7. 广告渠道统计：ads_channel_analysis
    //AdsChannelAnalysisReport.processData(pmtDF)


    //数据不再使用,释放资源
    pmtDF.unpersist()

//    Thread.sleep(1000000)
    spark.stop()

  }
}
