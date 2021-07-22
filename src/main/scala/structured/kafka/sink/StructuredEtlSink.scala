package structured.kafka.sink

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 实时从Kafka Topic消费基站日志数据，过滤获取通话转态为success数据，再存储至Kafka Topic中
 * 1、从KafkaTopic中获取基站日志数据（模拟数据，JSON格式数据）
 * 2、ETL：只获取通话状态为success日志数据
 * 3、最终将ETL的数据存储到Kafka Topic中
 */
object StructuredEtlSink {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.testing.memory", "471859200")
      .getOrCreate()

    import spark.implicits._
    /*
         //TODO: 导入隐式转换和函数库

    //    import org.apache.spark.sql.functions._
     */
    //1, 从kafka读取 数据
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "stationTopic")
      .load()

    //2 对基站日志数据进行ETL操作
    //station_0,1860000 314,18900002762,success,1626909387210,4000
    val etlStreamDF: Dataset[String] = kafkaStreamDF
      //获取value字段的值,转换为String类型
      .selectExpr("CAST(value as STRING)")
      //转换为Dataset类型
      .as[String]
      //过滤数据,通话状态为success
      .filter { log =>
        null != log && log.trim.split(",").length == 6 && "success".equals(log.trim.split(",")(3))
      }

    etlStreamDF.printSchema()

    // 3针对流式应用来说,输出的是流
    val query = etlStreamDF.writeStream.outputMode(OutputMode.Append())
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "etlTopic")
      //设置检查点目录
      .option("checkpointLocation", s"datas/structured/etl-100001")
      .start()

    println("running...")
    query.awaitTermination()
    query.stop() // 等待所有任务运行完成才停止运行
  }
}
