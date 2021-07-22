package structured.source

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * kafka-console-producer.sh --broker-list kafka:9092 --topic wordsTopic
 */
object StructuredKafkaSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory", "471859200")
      .getOrCreate()


    //TODO: 导入隐式转换和函数库
    import spark.implicits._
    //    import org.apache.spark.sql.functions._
    val kafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "wordsTopic")
      //设置每批次消费数据最大值
      .option("maxOffsetPerTrigger", "1000")
      .load()

    val resultStreamDF: DataFrame = kafkaStreamDF
      //获取value字段的值,转换为String类型
      .selectExpr("CAST(value AS STRING)")
      //转换为Dataset类型
      .as[String]
      //过滤数据
      .filter(line => null != line && line.trim.length > 0)
      //分割单词
      .flatMap(line => line.trim.split("\\s+"))
      .groupBy($"value").count()

    val query = resultStreamDF.writeStream.outputMode(OutputMode.Complete())
        .format("console").option("numRows","10")
        .option("truncate","false")
        .start()



    query.awaitTermination()
    query.stop() // 等待所有任务运行完成才停止运行

  }
}
