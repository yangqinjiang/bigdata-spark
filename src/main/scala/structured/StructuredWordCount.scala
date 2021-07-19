package structured

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果打印到控制台。
 */
object StructuredWordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory","471859200")
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
    /*
      root
      |-- value: string (nullable = true)
    */
    inputStreamDF.printSchema()

    //2 业务分析,词频统计wordCount
    val resultStreamDF: DataFrame = inputStreamDF.as[String]
      //过滤数据
      .filter(line => null != line && line.trim.length > 0)
      //分割单词
      .flatMap(line => line.trim.split("\\s+"))
      .groupBy($"value").count() //按照单词分组,聚合
    /*
      root
      |-- value: string (nullable = true)
      |-- count: long (nullable = false)
    */
    resultStreamDF.printSchema()

    // 3设置Streaming应用输出及启动
    val query: StreamingQuery = resultStreamDF.writeStream
      // TODO: 设置输出模式,Complete表示将ResultTable中所有结果数据输出
//      .outputMode(OutputMode.Complete())
      //TODO: 设置输出模式, Update表示将ResultTable中有更新结果输出
      .outputMode(OutputMode.Update())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      //流式应用,需要启动start
      .start()
    //流式查询,等待流式应用终止
    query.awaitTermination()
    //等待所有任务运行完成,才停止运行
    query.stop()

  }
}
