package sql

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import rt.config.ApplicationConfig

object StructuredRedis {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.testing.memory", "471859200")
      .config("spark.sql.shuffle.partitions", "2").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val inputStreamDF = spark.readStream.format("socket").option("host", "redis").option("port", 9999).load()

    val resultStreamDF: DataFrame = inputStreamDF.as[String].filter(line => null != line && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .groupBy($"value").count()

    val query: StreamingQuery = resultStreamDF.writeStream.outputMode(OutputMode.Update())
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF
          //TODO: 以下两行代码是   行转列  的作用
          .groupBy()
          .pivot($"value").sum("count")
          //添加一列
          .withColumn("type", lit("spark"))
          .write
          .mode(SaveMode.Append)
          .format("org.apache.spark.sql.redis")
          .option("host", ApplicationConfig.REDIS_HOST)
          .option("port", ApplicationConfig.REDIS_PORT)
          .option("dbNum", ApplicationConfig.REDIS_DB)
          .option("table", "wordcount")
          .option("key.column", "type")
          .save()
      }.start() // 启动start流式应用

    query.awaitTermination()
    query.stop()
  }
}
