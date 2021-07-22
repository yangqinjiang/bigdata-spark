package structured

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
 * * 基于Structured Streaming 模块读取TCP Socket读取数据，进行事件时间窗口统计词频WordCount，将结果打印到控制台
 * * TODO：每5秒钟统计最近10秒内的数据（词频：WordCount)
 * EventTime即事件真正生成的时间：
 * 例如一个用户在10：06点击 了一个按钮，记录在系统中为10：06
 * 这条数据发送到Kafka，又到了Spark Streaming中处理，已经是10：08，这个处理的时间就是process Time。
 * ** 测试数据：
 * 2019-10-12 09:00:02,cat dog
 * 2019-10-12 09:00:03,dog dog
 * 2019-10-12 09:00:07,owl cat
 * 2019-10-12 09:00:11,dog
 * 2019-10-12 09:00:13,owl
 */
object StructuredWindow {

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
      //将每行数据进行分割单词: 2019-10-12 09:00:02,cat dog
      .flatMap { line =>
        val arr = line.trim.split(",")
        arr(1).split("\\s+").map(word => (Timestamp.valueOf(arr(0)), word))
      }
      // 设置列的名称
      .toDF("insert_timestamp", "word")
      // TODO：设置基于事件时间（event time）窗口 -> insert_timestamp, 每5秒统计最近10秒内数据
      /*1. 先按照窗口分组、2. 再对窗口中按照单词分组、 3. 最后使用聚合函数聚合*/
      .groupBy(
        window($"insert_timestamp", "10 seconds", "5 seconds"), $"word"
      ).count()
      .orderBy($"window") //按照窗口字段降序排序

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
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "100")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    query.awaitTermination()
    query.stop()

  }
}
