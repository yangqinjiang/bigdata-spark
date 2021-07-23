package rt.store.es

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType}
import rt.config.ApplicationConfig
import rt.utils.{SparkUtils, StreamingUtils}

/**
 * StructuredStreaming 实时消费Kafka Topic中数据，存入到Elasticsearch索引中
 */
object RealTimeOrder2Es {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass)
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //2,从kafka读取消费数据
    val kafkaStreamDF: DataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      //从Kafka消费数据时，通过属性【maxOffsetsPerTrigger】，设置每批次最大数据量，实
      //际生产项目需要结合流式数据波峰及应用运行资源综合考虑设置；
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()

    //schema信息
    // {"orderId":"20200519161403348000001","userId":"200000291","orderTime":"2020-05-19 16:14:03
    // .348","ip":"222.68.105.201","orderMoney":"477.22","orderStatus":0,"province":"上海","city":"上海市"}
    val orderSchema: StructType = new StructType()
      .add("orderId",StringType,nullable = true)
      .add("userId",StringType,nullable = true)
      .add("orderTime",StringType,nullable = true)
      .add("ip",StringType,nullable = true)
      .add("orderMoney",StringType,nullable = true)
      .add("orderStatus",StringType,nullable = true)
      .add("province",StringType,nullable = true)
      .add("city",StringType,nullable = true)

    //3 获取订单记录数据
    val orderStreamDF = kafkaStreamDF
      //获取value字段的值, 转换为String类型
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      .select(from_json($"value", orderSchema).as("order"))
      .select($"order.*") // 从上面的select获取order字段

    orderStreamDF.printSchema()

    //4输出数据到ElasticSearch和启动流式应用
    val query = orderStreamDF.writeStream.outputMode(OutputMode.Append())
      .option("checkpointLocation", ApplicationConfig.STREAMING_ES_CKPT)
      .format("es")
      .option("es.nodes", ApplicationConfig.ES_NODES)
      .option("es.port", ApplicationConfig.ES_PORT)
      .option("es.index.auto.create", ApplicationConfig.ES_INDEX_AUTO_CREATE)
      .option("es.write.operation", ApplicationConfig.ES_WRITE_OPERATION)
      .option("es.mapping.id", ApplicationConfig.ES_MAPPING_ID)
      // 出现Spark连接不上es的问题
      //https://stackoverflow.com/questions/47651162/connecting-spark-and-elasticsearch
      .option("es.nodes.wan.only","true")
      .start(ApplicationConfig.ES_INDEX_NAME)

    StreamingUtils.stopStructuredStreaming(query,ApplicationConfig.STOP_ES_FILE)

  }
}
