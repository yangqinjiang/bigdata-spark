package rt.etl

import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{get_json_object, struct, to_json, udf}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}
import rt.config.ApplicationConfig
import rt.utils.{SparkUtils, StreamingUtils}

/**
 * 订单数据实时ETL：实时从Kafka Topic 消费数据，进行过滤转换ETL，将其发送Kafka Topic，以便实时处理
 * TODO：基于StructuredStreaming实现，Kafka作为Source和Sink
 * 上述代码中有两个细节，对于流式应用来说很关键：
 * 第一、从Kafka消费数据时，通过属性【maxOffsetsPerTrigger】，设置每批次最大数据量，实
 * 际生产项目需要结合流式数据波峰及应用运行资源综合考虑设置；
 *  第二、将ETL后数据保存至Kafka Topic中，设置检查点位置CheckpointLocation，便于流式应用
 * 运行失败后，可以从Checkpoint恢复，继续上次消费数据，进行实时处理；
 */
object RealTimeOrderETL extends Logging {

  /**
   * 对流式数据StreamDataFrame进行ETL过滤清洗转换操作
   *
   */
  def streamingProcess(streamDF: DataFrame): DataFrame = {

    val session = streamDF.sparkSession
    import session.implicits._

    // TODO: 对数据进行ETL操作，获取订单状态为0(打开)及转换IP地址为省份和城市
    // 1. 获取订单记录Order Record数据
    val recordStreamDS: Dataset[String] = streamDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      // 过滤数据：通话状态为success
      .filter(record => null != record && record.trim.split(",").length > 0)


    //缓存 文件
    session.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)
    // 自定义UDF函数 , 解析IP地址为省份和城市
    val ip_to_location: UserDefinedFunction = udf((ip: String) => {
      val dbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))
      val dataBlock: DataBlock = dbSearcher.btreeSearch(ip)
      val region: String = dataBlock.getRegion
      // 分割字符串，获取省份和城市
      val Array(_, _, province, city, _) = region.split("\\|")
      // 返回Region对象
      (province, city)
    })

    //2,其他订单字段,按照订单状态过滤和转换IP地址
    val resultStreamDF: DataFrame = recordStreamDS
      .select(
        // 提取订单字段
        // {"orderId":"20200518213916455000009","userId":"300000991","orderTime":"2020-05-18 21:3
        // 9:16.455","ip":"222.16.48.97","orderMoney":415.3,"orderStatus":0}
        get_json_object($"value", "$.orderId").as("orderId"),
        get_json_object($"value", "$.userId").as("userId"),
        get_json_object($"value", "$.orderTime").as("orderTime"),
        get_json_object($"value", "$.ip").as("ip"),
        get_json_object($"value", "$.orderMoney").as("orderMoney"),
        get_json_object($"value", "$.orderStatus").cast(IntegerType).as("orderStatus")
      )
      //过滤,订单状态为0(打开)
      .filter($"orderStatus" === 0)
      //解析IP地址为省份和城市
      .withColumn("location", ip_to_location($"ip"))
      //获取省份和城市列
      .withColumn("province", $"location._1")
      .withColumn("city", $"location._2")
      .select(
        // 从上面的数据,组合新数据, 输出为key, value
        $"orderId".as("key"),//输出为key,
        to_json(
          struct(
            $"orderId", $"userId", $"orderTime", $"ip",
            $"orderMoney", $"orderStatus", $"province", $"city"
          )
        ).as("value")//输出为 value
      )

    // 3返回
    resultStreamDF
  }

  def main(args: Array[String]): Unit = {
    //1 获取sparkSession实例对象
    val spark = SparkUtils.createSparkSession(this.getClass)
    //    implicit sparkSession.implicits._

    //2,从kafka读取消费数据
    val kafkaStreamDF: DataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_SOURCE_TOPICS)
      //从Kafka消费数据时，通过属性【maxOffsetsPerTrigger】，设置每批次最大数据量，实
      //际生产项目需要结合流式数据波峰及应用运行资源综合考虑设置；
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()

    //3 ETL操作
    val etlStreamDF = streamingProcess(kafkaStreamDF)

    //4 针对流式应用来说,输出的是流
    val query = etlStreamDF.writeStream.outputMode(OutputMode.Append())
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("topic", ApplicationConfig.KAFKA_ETL_TOPIC)
      //将ETL后数据保存至Kafka Topic中，设置检查点位置CheckpointLocation，便于流式应用
      //运行失败后，可以从Checkpoint恢复，继续上次消费数据，进行实时处理；
      .option("checkpointLocation", ApplicationConfig.STREAMING_ETL_CKPT) // 检查点目录
      .start()
//    query.awaitTermination()
//    query.stop()
    //优雅关闭停止StreamingQuery
    StreamingUtils.stopStructuredStreaming(query,ApplicationConfig.STOP_ETL_FILE)

  }
}
