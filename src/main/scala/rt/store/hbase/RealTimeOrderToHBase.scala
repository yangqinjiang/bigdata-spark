package rt.store.hbase

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import rt.config.ApplicationConfig
import rt.utils.{SparkUtils, StreamingUtils}

/**
 * StructuredStreaming 实时消费Kafka Topic中数据，存入到HBase表中
 */
object RealTimeOrderToHBase extends Logging {
  def main(args: Array[String]): Unit = {
    // 1. 获取SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    // 2. 从KAFKA读取消费数据
    val kafkaStreamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()
    // 3. 解析JSON格式数据，封装到DataFrame中，包含各个字段信息
    // 定义一个Schema信息
    val orderSchema: StructType = new StructType()
      .add("orderId", StringType, nullable = true)
      .add("userId", StringType, nullable = true)
      .add("orderTime", StringType, nullable = true)
      .add("ip", StringType, nullable = true)
      .add("orderMoney", StringType, nullable = true)
      .add("orderStatus", StringType, nullable = true)
      .add("province", StringType, nullable = true)
      .add("city", StringType, nullable = true)
    /*
    {"orderId":"20200818160415420000002","userId":"500000246","orderTime":"2020-08-18 16:04:1
    5.420","ip":"61.232.85.68","orderMoney":"222.52","orderStatus":0,"province":"浙江省","city":"杭州市"}
    */
    val orderStreamDS: Dataset[String] = kafkaStreamDF
      // 将value转换为String字符串类型
      .selectExpr("CAST(value AS STRING)")
      // 将DataFrame转换为Dataset
      .as[String]
      // 过滤数据
      .filter(line => null != line && line.trim.length > 0)
    // 4. 将数据保存至Elasticsearch索引中
    val query: StreamingQuery = orderStreamDS
      .toDF()
      .writeStream
      .queryName("query-store-hbase2")
      // 设置追加模式Append
      .outputMode(OutputMode.Append())
      // TODO: 调用foreachBatch函数，将每批次DataFrame数据存储至HBase表中
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        logWarning("foreachBath")
        batchDF.rdd.foreachPartition { iter =>
          val datas: Iterator[String] = iter.map(row => row.getAs[String]("value"))
          datas.foreach(println)
          val isInsertSuccess: Boolean = HBaseDao.insert(
            ApplicationConfig.HBASE_ORDER_TABLE,
            ApplicationConfig.HBASE_ORDER_TABLE_FAMILY,
            ApplicationConfig.HBASE_ORDER_TABLE_COLUMNS,
            datas
          )
          logWarning(s"Insert Datas To HBase: $isInsertSuccess")
        }
      }

      // 设置检查点目录
      .option("checkpointLocation", "datas/order-apps/ckpt/hbase-ckpt2/")
      .start()

    // TODO: 5. 通过扫描HDFS文件，优雅的关闭停止StreamingQuery
    StreamingUtils.stopStructuredStreaming(query, "datas/order-apps/stop/hbase-stop2")
  }
}
