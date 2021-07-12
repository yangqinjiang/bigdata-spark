package offline.etl

import offline.config.ApplicationConfig
import offline.utils.{IpUtils, SparkUtils}
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}
import org.apache.spark.sql.functions._
// 1. 创建SparkSession实例对象
// 2. 加载json数据
// 3. 解析IP地址为省份和城市
// 4. 保存ETL数据至Hive分区表
// 5. 应用结束，关闭资源
//基于SparkSQL中DataFrame数据结构，使用DSL编程方式

object PmtEtlRunner {

  /**
   * 对数据进行etl处理,调用ip2Region第三方库,解析IP地址为省份和城市
   *
   * @param df
   * @return
   */
  def processData(df1: DataFrame): DataFrame = {

    //获取sparkSession对象,并导入隐式转换
    val spark = df1.sparkSession
    import spark.implicits._

    //解析IP地址数据字典文件分发
    spark.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)
    //由于DataFrame弱类型(无泛型),不能直接使用mapPartitions或map,建议转换为RDD操作

    // a. 解析IP地址
    val newRowsRDD: RDD[Row] = df1.rdd.mapPartitions { iter =>
      //创建DbSearcher对象,针对每个分区创建一个,并不是每条数据创建一个
      val dbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))
      //针对每个分工数据操作,获取IP值,解析为省份和城市
      iter.map { row =>
        // 获取ip值
        val ipValue: String = row.getAs[String]("ip")
        // 调用工具类解析IP地址
        val region: Region = IpUtils.convertIpToRegion(ipValue, dbSearcher)
        // 将解析省份和城市append到原来的row中
        val newSeq = row.toSeq :+ region.province :+ region.city
        // 返回row对象
        Row.fromSeq(newSeq)

      }


    }
    // b,自定义schema信息
    val newSchema: StructType = df1.schema // 获取原来DataFrame中Schema信息
      // 添加新字段Schema信息
      .add("province", StringType, nullable = true)
      .add("city", StringType, nullable = true)
    // c. 将RDD转换为DataFrame
    val df: DataFrame = spark.createDataFrame(newRowsRDD, newSchema)
    // d. 添加一列日期字段，作为分区列
    df.withColumn("date_str", date_sub(current_date(), 1).cast(StringType))
  }

  def main(args: Array[String]): Unit = {
    //设置Spark应用程序运行的用户,root ,默认情况下为当前系统用户
    System.setProperty("user.name", "root")
    System.setProperty("HADOOP_USER_NAME", "root")
    //1 创建sparkSession实例对象
    val spark = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    //2,加载json数据
    val pmtDF: DataFrame = spark.read.json(ApplicationConfig.DATAS_PATH)
    pmtDF.printSchema()
    pmtDF.show(10, truncate = false)

    //3解析IP地址为省份和城市
    val etlDF: DataFrame = processData(pmtDF)

    etlDF.printSchema()
    etlDF.select($"ip", $"province", $"city", $"date_str").show(10, truncate = false)
    spark.stop()
  }
}
