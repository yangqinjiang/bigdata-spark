package structured

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
/**
 * 基于Structured Streaming 读取TCP Socket读取数据，事件时间窗口统计词频，将结果打印到控制台
 * * TODO：每5秒钟统计最近10秒内的数据（词频：WordCount)，设置水位Watermark时间为10秒
 *      dog,2019-10-10 12:00:07
 *      owl,2019-10-10 12:00:08
 *      dog,2019-10-10 12:00:14
 *      cat,2019-10-10 12:00:09
 *      cat,2019-10-10 12:00:15
 *      dog,2019-10-10 12:00:08
 *      owl,2019-10-10 12:00:13
 *      owl,2019-10-10 12:00:21
 *      owl,2019-10-10 12:00:17
 *
 */
object StructuredWatermarkUpdate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory", "471859200")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    // 使用sparkSession从tcp socket读取流式数据
    val inputStreamDF = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    //针对获取流式DStream进行词频统计

    val resultStreamDF = inputStreamDF
      //将dataframe转换为dataset操作, dataset是类型安全,强类型
      .as[String]
      .filter(line => null != line && line.trim.length > 0)
      //将每行数据进行分割单词: cat dog,2019-10-12 09:00:02
      .flatMap { line =>
        val arr = line.trim.split(",")
        arr(0).split("\\s+").map(word => (word,Timestamp.valueOf(arr(1))))
      }
      // 设置列的名称
      .toDF("word", "time")
      //设置水位watermark
      .withWatermark("time","10 seconds")
      // TODO：设置基于事件时间（event time）窗口 -> insert_timestamp, 每5秒统计最近10秒内数据
      /*1. 先按照窗口分组、2. 再对窗口中按照单词分组、 3. 最后使用聚合函数聚合*/
      .groupBy(
        window($"time", "10 seconds", "5 seconds"), $"word"
      ).count()

    /**
     * root
     * |-- window: struct (nullable = true)
     * |    |-- start: timestamp (nullable = true)
     * |    |-- end: timestamp (nullable = true)
     * |-- word: string (nullable = true)
     * |-- count: long (nullable = false)
     */
    resultStreamDF.printSchema()

    // 4. 将计算的结果输出，打印到控制台
    val query: StreamingQuery = resultStreamDF.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("numRows", "100")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    query.awaitTermination()
    query.stop()
  }
}
