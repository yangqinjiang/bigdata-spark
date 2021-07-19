package structured.sink

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果存储到MySQL数据库表中
 */
object StructuredMySQLSink {

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


    //1,从tcp socket读取数据
    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    //2 业务分析,词频统计wordCount
    val resultStreamDF: DataFrame = inputStreamDF.as[String]
      //过滤数据
      .filter(line => null != line && line.trim.length > 0)
      //分割单词
      .flatMap(line => line.trim.split("\\s+"))
      .groupBy($"value").count() //按照单词分组,聚合

    // 3设置Streaming应用输出及启动
    val query: StreamingQuery = resultStreamDF.writeStream

      .outputMode(OutputMode.Update())
      .foreach(new MySQLForeachWriter())
      //流式应用,需要启动start
      .start()
    // 查询器等待流式应用终止
    query.awaitTermination()
    query.stop() // 等待所有任务运行完成才停止运行
  }
}
