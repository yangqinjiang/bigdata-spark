package rt.report

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import rt.config.ApplicationConfig
import rt.utils.{JedisUtils, SparkUtils, StreamingUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, StringType}

/**
 * 实时订单报表：从Kafka Topic实时消费订单数据，进行销售订单额统计，维度如下：
 * - 第一、总销售额：sum
 * - 第二、各省份销售额：province
 * - 第三、重点城市销售额：city
 * "北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市" * TODO：每日实时统计，每天统计的数据为当日12:00 - 24：00产生的数据
 */
object DailyRealTimeOrderReport extends Logging {

  //将数据DataFrame保存到redis数据库
  def savetoRedis(batchDF: DataFrame): Unit = {
    batchDF.rdd.foreachPartition { iter =>
      //1 获取Jedis连接对象
      val jedis = JedisUtils.getJedisPoolInstance(ApplicationConfig.REDIS_HOST, ApplicationConfig.REDIS_PORT.toInt).getResource
      jedis.select(ApplicationConfig.REDIS_DB.toInt) //选择数据库DB
      // 2将每个分区数据插入redis中
      iter.foreach { row =>
        //获取key,field和value字段的值
        val redisKey = row.getAs[String]("key")
        val redisField = row.getAs[String]("field")
        val redisValue = row.getAs[String]("value")
        println(s"key = $redisKey, field = $redisKey, value = $redisValue")
        //调用hset插入redis哈希中
        jedis.hset(redisKey, redisField, redisValue)
      }
      //3 释放资源
      JedisUtils.release(jedis)
    }
  }

  //实时统计,总销售额, 使用sum函数
  def reportAmtTotal(streamDF: DataFrame): Unit = {
    logWarning("call reportAmtTotal...")
    // a. 导入隐式转换
    import streamDF.sparkSession.implicits._
    // b 业务计算
    val resultStreamDF = streamDF
      //设置水位Watermark阈值
      .withWatermark("order_timestamp", "10 minutes")
      // 按照订单日期分组
      .groupBy($"order_date")
      //累加统计订单销售额总额
      .agg(sum($"money").as("total_amt"))
      .withColumn("prefix", lit("orders:money:total")) // redis的key前缀
      .withColumn("field", lit("global"))
      .select(
        concat_ws(":", $"prefix", $"order_date").as("key"),
        $"field", //
        $"total_amt".cast(StringType).as("value") // 将double转换为string
      )
    //c 输出redis及启动流式应用
    resultStreamDF.writeStream.outputMode(OutputMode.Update())
      .queryName("query-daily-amt-total")
      //设置检查点目录
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_DAILY_TOTAL_CKPT)
      //输出到redis
      .foreachBatch { (batchDF: DataFrame, _: Long) => savetoRedis(batchDF) }
      //流式应用,需要启动start
      .start()
  }

  //实时统计,各省份销售额,按照province省份分组
  def reportAmtProvince(streamDF: DataFrame): Unit = {
    logWarning("call reportAmtProvince...")
    // a. 导入隐式转换
    import streamDF.sparkSession.implicits._
    // b 业务计算
    val resultStreamDF = streamDF
      //设置水位Watermark阈值
      .withWatermark("order_timestamp", "10 minutes")
      // 按照订单日期order_date,province分组,再求和
      .groupBy($"order_date", $"province")
      //累加统计订单销售额总额
      .agg(sum($"money").as("total_amt"))
      .withColumn("prefix", lit("orders:money:province")) // redis的key前缀

      .select(
        concat_ws(":", $"prefix", $"order_date").as("key"),
        $"province".as("field"), // province 作为field值
        $"total_amt".cast(StringType).as("value") // 将double转换为string
      )
    //c 输出redis及启动流式应用
    resultStreamDF.writeStream.outputMode(OutputMode.Update())
      .queryName("query-daily-amt-province")
      //设置检查点目录
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_DAILY_PROVINCE_CKPT)
      //输出到redis
      .foreachBatch { (batchDF: DataFrame, _: Long) => savetoRedis(batchDF) }
      //流式应用,需要启动start
      .start()
  }

  /** 实时统计：重点城市销售额，按照city城市分组 */
  def reportAmtCity(streamDF: DataFrame): Unit = {
    logWarning("call reportAmtCity...")
    // a. 导入隐式转换
    import streamDF.sparkSession.implicits._
    // 重点城市：9个城市
    val cities: Array[String] = Array(
      "北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市")
    val citiesBroadcast: Broadcast[Array[String]] = streamDF.sparkSession.sparkContext.broadcast(cities)
    // 自定义UDF函数，判断是否重点城市
    val city_is_contains: UserDefinedFunction = udf(
      (cityName: String) => citiesBroadcast.value.contains(cityName)
    )

    // b 业务计算
    val resultStreamDF = streamDF
      //过滤获取重点省份订单
      .filter(city_is_contains($"city"))
      //设置水位Watermark阈值
      .withWatermark("order_timestamp", "10 minutes")
      // 按照订单日期order_date,city分组,再求和
      .groupBy($"order_date", $"city")
      //累加统计订单销售额总额
      .agg(sum($"money").as("total_amt"))
      .withColumn("prefix", lit("orders:money:city")) // redis的key前缀

      .select(
        concat_ws(":", $"prefix", $"order_date").as("key"),
        $"city".as("field"), // province 作为field值
        $"total_amt".cast(StringType).as("value") // 将double转换为string
      )
    //c 输出redis及启动流式应用
    resultStreamDF.writeStream.outputMode(OutputMode.Update())
      .queryName("query-daily-amt-city")
      //设置检查点目录
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_DAILY_CITY_CKPT)
      //输出到redis
      .foreachBatch { (batchDF: DataFrame, _: Long) => savetoRedis(batchDF) }
      //流式应用,需要启动start
      .start()
  }

  def main(args: Array[String]): Unit = {
    //1 获取sparkSession实例对象
    val spark = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    //2,从kafka读取 消费数据
    val kafkaStreamDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      //设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()

    //3 提供数据字段
    val orderStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING)")
      .as[String]
      //过滤数据, 通话状态为success
      .filter(record => null != record && record.trim.split(",").length > 0)
      //提取字段,orderTime,orderMoney,province和city
      .select(
        to_date(get_json_object($"value", "$.orderTime")).as("order_date"), //将订单的日期时间组合值 转换为日期,
        to_timestamp(get_json_object($"value", "$.orderTime")).as("order_timestamp"), //转换为时间戳
        // 解决Double精度丢失的问题
        get_json_object($"value", "$.orderMoney").cast(DataTypes.createDecimalType(10, 2)).as("money"),
        get_json_object($"value", "$.province").as("province"),
        get_json_object($"value", "$.city").as("city")
      )

    //4,实时报表统计, 总销售额, 各省份销售额及重点城市销售额
    reportAmtTotal(orderStreamDF)
    reportAmtProvince(orderStreamDF)
    reportAmtCity(orderStreamDF)

    //5 定时扫描HDFS文件, 优雅的关闭停止StreamingQuery
    spark.streams.active.foreach { query =>
      StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_STATE_FILE)

    }
  }
}
