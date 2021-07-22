package rt.utils

import rt.config.ApplicationConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkUtils {

  //构建SparkSession实例对象步骤：
//  1. 构建SparkConf对象、设置通用相关属性
//  2. 判断应用是否本地模式运行，如果是设置值master
//  3. 获取SparkSession实例对象
  /**
   * 获取SparkSession实例对象，传递Class对象
   * @param clazz Spark Application字节码Class对象
   * @return SparkSession对象实例
   */
  def createSparkSession(clazz: Class[_]):SparkSession = {

    val sparkConf :SparkConf = new SparkConf()
      .setAppName(clazz.getSimpleName.stripSuffix("$"))
      .set("spark.debug.maxToStringFields","2000")
      .set("spark.sql.debug.maxToStringFields","2000")

    // 2,判断应用是否本地模式运行,如果是设置值
    if(ApplicationConfig.APP_LOCAL_MODE){
      sparkConf
        .setMaster(ApplicationConfig.APP_SPARK_MASTER)
        //设置shuffle分区数目
        .set("spark.sql.shuffle.partitions","3")
    }

    //3 获取sparkSession实例对象
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session
  }
  /**
   * 获取StreamingContext流式上下文实例对象
   * @param clazz Spark Application字节码Class对象
   * @param batchInterval 每批次时间间隔
   */
  def createStreamingContext(clazz:Class[_],batchInterval:Int):StreamingContext = {

    //构建对象实例
    val context = StreamingContext.getActiveOrCreate(
      () => {
        //1,构建sparkConf对象
        val sparkConf = new SparkConf().setAppName(clazz.getSimpleName.stripSuffix("$"))
          .set("spark.debug.maxToStringFields", "2000")
          .set("spark.sql.debug.maxToStringFields", "2000")
          .set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅停止spark服务

        //其中应用开发本地模式运行时设置的相关属性，在测试和生成环境使用spark-submit提交应用，
        //通过--conf指定此属性的值
        if (ApplicationConfig.APP_LOCAL_MODE) {
          sparkConf.setMaster(ApplicationConfig.APP_SPARK_MASTER)
            //设置每批次消费数据最大数据量, 生成环境使用命令行设置
            .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        }
        //3, 创建StreamingContext对象
        new StreamingContext(sparkConf, Seconds(batchInterval))
      }
    )
    context // 返回对象
  }
}
