package offline.utils

import offline.config.ApplicationConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//构建SparkSession实例对象工具类,加载配置属性
object SparkUtils {

  /**
   * 构建SparkSession实例对象
   * @param clazz 应用Class对象,获取应用类名称
   * @return SparkSession实例对象
   */
  def createSparkSession(clazz:Class[_]):SparkSession = {
    val sparkConf = new SparkConf().setAppName(clazz.getSimpleName.stripSuffix("$"))
    //设置输出文件算法
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.debug.maxToStringFields", "20000")
      .set("spark.testing.memory","471859200")

    //2 判断应用是否本地模式运行,如果是设置值
    if(ApplicationConfig.APP_LOCAL_MODE){
      sparkConf.setMaster(ApplicationConfig.APP_SPARK_MASTER)
      //设置shuffle时分区数目
        .set("spark.sql.shuffle.partitions", "4")
    }

    //3 创建SparkSession.Builder对象
    val builder = SparkSession.builder().config(sparkConf)

    //4判断 应用是否集成Hive,如果集成,设置HiveMetaStore地址
    if(ApplicationConfig.APP_IS_HIVE){
      builder
        .enableHiveSupport()
        .config("hive.metastore.uris", ApplicationConfig.APP_HIVE_META_STORE_URLS)
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
    }

    // 5获取SparkSession实例对象
    val session = builder.getOrCreate()
    session
  }
}
