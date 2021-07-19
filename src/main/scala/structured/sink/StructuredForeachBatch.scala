package structured.sink

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果存储到MySQL数据库表中
 */
object StructuredForeachBatch {

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
//    设置输出模式：Complete表示将ResultTable中所有结果数据输出
      .outputMode(OutputMode.Complete())
      .foreachBatch{(batchDF:DataFrame,batchId:Long) =>{
        println(s"BatchId = ${batchId}")
        if(!batchDF.isEmpty){
          // 降低分区数目,保存数据到mysql表
          batchDF.coalesce(1).write.mode(SaveMode.Overwrite)
            .format("jdbc")
            .option("driver","com.mysql.cj.jdbc.Driver")
            .option("url","jdbc:mysql://localhost:3308/db_spark?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
            .option("user","root")
            .option("password","")
            .option("dbtable","db_spark.tb_word_count2")
            .save()
        }
      }}
      //流式应用,需要启动start
      .start()
    // 查询器等待流式应用终止
    query.awaitTermination()
    query.stop() // 等待所有任务运行完成才停止运行
  }
}
