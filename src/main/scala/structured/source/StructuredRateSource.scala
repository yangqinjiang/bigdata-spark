package structured.source

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredRateSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory", "471859200")
      .getOrCreate()

    /**
     * // 导入隐式转换和函数库
     * import spark.implicits._
     * import org.apache.spark.sql.functions._
     */

    //从Rate数据源实时消费数据
    val rateStreamDF: DataFrame = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10") // 每秒生成数据数目
      .option("rampUpTime", "0s") //每条数据生成间隔时间
      .option("numPartitions", "2") //分区数目
      .load()

    //3 设置streaming应用输出及启动
    val query:StreamingQuery = rateStreamDF.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .option("numRows","10")
        .option("truncate","false")
        .start()
    // 查询器等待流式应用终止
    query.awaitTermination()
    query.stop() // 等待所有任务运行完成才停止运行
  }
}
