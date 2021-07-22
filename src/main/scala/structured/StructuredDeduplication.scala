package structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

object StructuredDeduplication {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory", "471859200")
      .getOrCreate()
    // 使用sparkSession从tcp socket读取流式数据
    val inputStreamDF = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val resultStreamDF = inputStreamDF.as[String]
      .filter(line => null != line && line.trim.length > 0)
      .select(
        get_json_object($"value", "$.eventTime").as("event_time"),
        get_json_object($"value", "$.eventType").as("event_type"),
        get_json_object($"value", "$.userID").as("user_id")
      )
      //按照UserId和EventType去重
      .dropDuplicates("user_id", "event_type")
      .groupBy($"user_id", $"event_type")
      .count()

    // 4. 将计算的结果输出，打印到控制台
    val query: StreamingQuery = resultStreamDF.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .start()
    query.awaitTermination()
    query.stop()
  }
}
