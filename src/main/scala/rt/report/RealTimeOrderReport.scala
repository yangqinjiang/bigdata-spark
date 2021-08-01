package rt.report

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import rt.config.ApplicationConfig
import rt.utils.{SparkUtils, StreamingUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.DoubleType

/**
 * 实时订单报表：从Kafka Topic实时消费订单数据，进行销售订单额统计，结果实时存储Redis数据库，维度如下：
 * - 第一、总销售额：sum
 * - 第二、各省份销售额：province
 * - 第三、重点城市销售额：city
 * "北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市"
 */
object RealTimeOrderReport extends Logging{

  /**
   * 实时统计,总销售额,使用sum函数
   *
   * @param streamDF
   * @return
   */
  def reportAmtTotal(streamDF: DataFrame) = {
    // 导入隐式转换
    import streamDF.sparkSession.implicits._
    //业务 计算 ， DataFrame = Dataset[Row]
    val resultStreamDF: Dataset[Row] = streamDF
      .agg(sum($"money").as("total_amt"))
      .withColumn("total", lit("global"))
    logWarning("call reportAmtTotal...")
    // 输出redis及启动流式应用
    resultStreamDF.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-total")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_TOTAL_CKPT)
      //结果输出 到redis
      .foreachBatch {
        (batchDF: DataFrame, _: Long) =>
          batchDF.coalesce(1)
            .groupBy()
            .pivot($"total").sum("total_amt")
            .withColumn("type", lit("total"))
            .write.mode(SaveMode.Append)
            .format("org.apache.spark.sql.redis")
            .option("host", ApplicationConfig.REDIS_HOST)
            .option("port", ApplicationConfig.REDIS_PORT)
            .option("dbNum", ApplicationConfig.REDIS_DB)
            .option("table", "orders:money")
            .option("key.column", "type")
            .save() // 流式应用，需要启动start

      }.start()
  }

  def reportAmtProvince(streamDF: DataFrame) = {
    // 导入隐式转换
    import streamDF.sparkSession.implicits._
    //业务 计算 ， DataFrame = Dataset[Row]
    val resultStreamDF: Dataset[Row] = streamDF
      //按照省份province分组,求和
        .groupBy($"province")
      .agg(sum($"money").as("total_amt"))

    // 输出redis及启动流式应用
    resultStreamDF.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-province")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_PROVINCE_CKPT)
      //结果输出 到redis
      .foreachBatch {
        (batchDF: DataFrame, _: Long) =>
          batchDF.coalesce(1)
            .groupBy()
            .pivot($"province").sum("total_amt")
            .withColumn("type", lit("province"))
            .write.mode(SaveMode.Append)
            .format("org.apache.spark.sql.redis")
            .option("host", ApplicationConfig.REDIS_HOST)
            .option("port", ApplicationConfig.REDIS_PORT)
            .option("dbNum", ApplicationConfig.REDIS_DB)
            .option("table", "orders:money")
            .option("key.column", "type")
            .save() // 流式应用，需要启动start
      }.start()
  }

  //实时统计, 重点城市cities销售额,按照city城市分组
  def reportAmtCity(streamDF: DataFrame) = {
    val session:SparkSession=streamDF.sparkSession
    // 导入隐式转换
    import streamDF.sparkSession.implicits._

    val cities:Array[String] = Array("北京市","上海市","深圳市","广州市","杭州市","成都市","南京市","武汉市","西安市")

    //广播变量
    val citiesBroadcast: Broadcast[Array[String]] = session.sparkContext.broadcast(cities)
    //自定义UDF函数, 判断 是否重点城市
    val city_is_contains: UserDefinedFunction = udf((cityName: String) => citiesBroadcast.value.contains(cityName))
    //业务 计算 ， DataFrame = Dataset[Row]
    val resultStreamDF: Dataset[Row] = streamDF
        .filter(city_is_contains($"city")) //过滤重点省份订单
      //按照省份province分组,求和
      .groupBy($"city")
      .agg(sum($"money").as("total_amt"))

    // 输出redis及启动流式应用
    resultStreamDF.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-city")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_CITY_CKPT)
      //结果输出 到redis
      .foreachBatch {
        (batchDF: DataFrame, _: Long) =>
          batchDF.coalesce(1)
            .groupBy()
            .pivot($"city").sum("total_amt")
            .withColumn("type", lit("city"))// 添加一列, 统计类型
            .write.mode(SaveMode.Append)
            .format("org.apache.spark.sql.redis")
            .option("host", ApplicationConfig.REDIS_HOST)
            .option("port", ApplicationConfig.REDIS_PORT)
            .option("dbNum", ApplicationConfig.REDIS_DB)
            .option("table", "orders:money")
            .option("key.column", "type")
            .save() // 流式应用，需要启动start
      }.start()
  }

  def main(args: Array[String]): Unit = {
    //1 获取sparkSession实例对象
    val spark = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    //2 从kafka读取消费数据
    val kafkaStreamDF: DataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      //设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()

    val orderStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING)")
      .as[String]
      //过滤数据
      .filter(record => null != record && record.trim.split(",").length > 0)
      //提取字段,orderMoney,province,city
      .select(
        get_json_object($"value", "$.orderMoney").cast(DoubleType).as("money"),
        get_json_object($"value", "$.province").as("province"),
        get_json_object($"value", "$.city").as("city")
      )
    //4实时报表统计, 总销售额, 各省份销售额及重点城市销售额
    reportAmtTotal(orderStreamDF)
    reportAmtProvince(orderStreamDF)
    reportAmtCity(orderStreamDF)

    //上面有多个stream, 使用下面的代码,遍历地关闭
    //    此 StructuredStreaming 应 用 中包 含 三 个 StreamingQuery ，每个 StreamingQuery 都设置
    //    Checkpoint Location 用 于 容 灾 恢 复 ； 此 外 ， 包 含 多 个 StreamingQuery 时 ， 调 用
    //    【spark.streams.active】获取所有正在运行的StreamingQuery。
    spark.streams.active.foreach { query =>
      StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_STATE_FILE)

    }


  }
}
