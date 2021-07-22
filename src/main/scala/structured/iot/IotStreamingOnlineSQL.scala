package structured.iot

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.streaming.OutputMode

/**
 * 对物联网设备状态信号数据，实时统计分析，基于SQL编程
 * 1）、信号强度大于30的设备
 * 2）、各种设备类型的数量
 * 3）、各种设备类型的平均信号强度
 */
object IotStreamingOnlineSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.testing.memory", "471859200")
      .getOrCreate()
    import spark.implicits._
    //    import org.apache.spark.sql.functions._

    //1, 从kafka读取 数据
    val iotStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "iotTopic")
      //设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", "1000")
      .load()
    // 3. 对获取数据进行解析，封装到DeviceData中
    val etlStreamDF = iotStreamDF.selectExpr("CAST(value as STRING)")
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      //解析JSON数据: {"device":"device_80","deviceType":"bigdata","signal":93.0,"time":1626911594144}
      .select(
        get_json_object($"value", "$.device").as("device_id"),
        get_json_object($"value", "$.deviceType").as("device_type"),
        get_json_object($"value", "$.signal").as("signal"),
        get_json_object($"value", "$.time").as("time")
      )

    // 4. 依据业务，分析处理
    // TODO: signal > 30 所有数据，按照设备类型 分组，统计数量、平均信号强度
    // 4.1 注册DataFrame为临时视图
    etlStreamDF.createOrReplaceTempView("view_tmp_stream_iots")

    //4.2编写SQL执行查询
    val resultStreamDF = spark.sql(
      """
        |SELECT device_type,COUNT(device_type) as count_device,ROUND(AVG(signal),2) AS avg_signal
        |FROM view_tmp_stream_iots
        |WHERE signal > 30 GROUP BY device_type
        |""".stripMargin)

    val query = resultStreamDF.writeStream.outputMode(OutputMode.Complete())
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println("===========================================")
        println(s"BatchId = ${batchId}")
        println("===========================================")
        if (!batchDF.isEmpty) batchDF.coalesce(1).show(20, truncate = false)
      }.start()

    query.awaitTermination()
    query.stop() // 等待所有任务运行完成才停止运行
  }
}
