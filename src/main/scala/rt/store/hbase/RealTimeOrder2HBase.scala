package rt.store.hbase

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import rt.config.ApplicationConfig
import rt.utils.{SparkUtils, StreamingUtils}

/**
 * SparkStreaming实时消费kafkaTopic中数据,存入到HBase表中
 * TODO: 相关说明
 * -a, kafka中一个topic中的数据,存储在一个HBase表中
 * -b, 所有topic中的每条数据,都是JSON格式的数据
 */
object RealTimeOrder2HBase extends Logging {

  // 从数据源读取 流式数据,经过状态操作分析数据,最终将数据输出
  def streamingProcess(ssc: StreamingContext): Unit = {
    val groupId: String = ApplicationConfig.STREAMING_ETL_GROUP_ID // 消费者GroupID
    val topics: Array[String] = ApplicationConfig.KAFKA_ETL_TOPIC.split(",")
    //1 从kafka topic 实时消费数据
    val kafkaDStream: InputDStream[String] = {
      //1)从kafka中读取数据的相关配置信息
      val kafkaParams: Map[String, String] = Map(
        "bootstrap.servers" -> ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS,
        "auto.offset.reset" -> ApplicationConfig.KAFKA_AUTO_OFFSET_RESET
      )
      // 2从zookeeper中加载各个topic中各个分区的offset信息
      val fromOffsets = ZkOffsetsUtils.loadFromOffsets(topics, groupId)
      logWarning("===================== Start Streaming From Offsets ====================")
      logWarning(s"${fromOffsets.mkString(", ")}")

      // 3) 从Kafka Topic中获取每条数据以后的处理方式，此处获取Offset和Message（Value）
      val messageHandler = (mam: MessageAndMetadata[String, String]) => {
        logWarning("1----------><------------")
        mam.message()
      }
      logWarning("2----------><------------")
      // 4) 采用Direct方式从Kafka Topic中pull拉取数据
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
        ssc, kafkaParams, //
        fromOffsets, //
        messageHandler //
      )

    }
    logWarning("3----------><------------")
//    kafkaDStream.print(10)

    kafkaDStream.print(10)
    logWarning("4----------><------------")
    // 2. 将获取的数据，插入到HBase表中
    // kafkaDStream 直接从Kafka Topic中获取的数据，KafkaRDD就是直接获取的Topic的数据，未进行任何处理
    if(false)kafkaDStream.foreachRDD { (rdd, time) =>
      logWarning(s"=======================${time.milliseconds}=======================")
      // =========================================================================
      if (!rdd.isEmpty()) {
        //获取当前批次RDD中各个分区数据的偏移范围(KAFKA topic中各个分区数据对应到RDD中各个分区的数据
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // TODO: 基于RDD的每个分区进行操作，将数据批量存储到HBase表中，使用Put方式
        rdd.foreachPartition { iter =>
          // 插入数据到HBase
          val isInsertSuccess = HBaseDao.insert(
            ApplicationConfig.HBASE_ORDER_TABLE,
            ApplicationConfig.HBASE_ORDER_TABLE_FAMILY,
            ApplicationConfig.HBASE_ORDER_TABLE_COLUMNS,
            iter
          )
          // 获取当前分区ID（RDD中每个分区的数据被一个Task处理，每个Task处理数据的时候有一个上下文对象TaskContext）
          val partitionId: Int = TaskContext.getPartitionId()
          // 依据分区ID, 获取到分数据中对应Kafka Topic中分区数据的偏移量
          val offsetRange = offsetRanges(partitionId)
          logWarning(s"${offsetRange.topic} - ${offsetRange.partition}: from [${offsetRange.fromOffset}], to [${offsetRange.untilOffset}]")
          // 当将分区的数据插入到HBase表中成功以后，更新Zookeeper上消费者偏移量
          if (isInsertSuccess) ZkOffsetsUtils.saveUtilOffsets(offsetRange, groupId)
        }
      }
      // =========================================================================

    }
  }

  def main(args: Array[String]): Unit = {
    //1,构建流式上下文StreamingContext实例对象
    val ssc: StreamingContext = {
      SparkUtils.createStreamingContext(this.getClass, 5)
    }

    //2从kafka实时消费数据,经过处理以后,存储HBase表
    /*
      表的名称：htb_orders
      RowKey：userId_orderTime，可以满足基于用户查询订单数据，同时加上日期时间范围查询
      列簇：info
      列：所有列，JSON中所有字段
    */
    streamingProcess(ssc)

    //3启动流式应用, 实时消费数据并进行处理
    ssc.start()
    // TODO: 通过扫描监控文件，优雅的关闭停止StreamingContext流式应用
    // 设置参数spark.streaming.stopGracefullyOnShutdown为true，优雅的关闭
    StreamingUtils.stopStreaming(ssc, ApplicationConfig.STOP_HBASE_FILE)
  }
}
