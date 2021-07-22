package structured

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 从Spark 2.3版本开始，StructuredStreaming结构化流中添加新流式数据处理方式：Continuous processing
 * 持续流数据处理：当数据一产生就立即处理，类似Storm、Flink框架，延迟性达到100ms以下，目前属于实验开发阶段
 */
// TODO: 首先运行structured.kafka.mock.MockStationLog类, 产生数据
object StructuredContinuous {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory", "471859200")
      .getOrCreate()

    import spark.implicits._
    //1, 从kafka读取 数据
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "stationTopic")
      .load()

    // 2对基站日志数据进行ETL操作
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
      .option("checkpointLocation", s"datas/structured/etl-100002")
      // TODO: 设置持续流处理 Continuous Processing, 指定CKPT时间间隔
      /*
      the continuous processing engine will records the progress of the query every second
      持续流处理引擎，将每1秒中记录当前查询Query进度状态
      */
      .trigger(Trigger.Continuous("1 second"))
      .start()

    println("running...")
    query.awaitTermination()
    query.stop() // 等待所有任务运行完成才停止运行
  }
}
