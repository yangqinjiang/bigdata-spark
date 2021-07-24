package rt.store.hbase

import kafka.common.TopicAndPartition
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
import rt.config.ApplicationConfig


/**
 * SparkStreaming从Kafka消费数据，将偏移量保存Zookeeper上，提供加载读取和保存写入方法
 */
object ZkOffsetsUtils {

  /**
   * 依据Topic名称，到Zookeeper上查找分区数，再到ZK Cluster中获取消费偏移量
   *
   * @return Map集合，各个Topic对应各个分区及对应消费偏移量
   */
  def loadFromOffsets(topics: Array[String], groupId: String): Map[TopicAndPartition, Long] = {
    //a,构建ZkClient实例对象,用于连接 ZooKeeper并且操作
    val zkClient: ZkClient = new ZkClient(
      ApplicationConfig.KAFKA_ZK_URL, //zkServers
      30000, //sessionTimeout
      30000, //connectionTimeout
      ZKStringSerializer //序列化
    )
    //b 组装offset,从zookeeper读取
    import scala.collection.mutable
    val fromOffsets: mutable.Map[TopicAndPartition, Long] = mutable.Map[TopicAndPartition, Long]()
    //c,针对每个topic获取所有分区partition的偏移量
    topics.foreach { topicName =>
      //i, 获取Topic有多少个分区,需要到zookeeper上读取, 通过zkClient获取某个ZNode孩子数
      //路径基于Kafka chroot生成,: /brokers/topics/testTopic/partitions
      val children: Int = zkClient.countChildren(ZkUtils.getTopicPartitionPath(topicName))
      // ii. 依据分区，到Zookeeper中获取对应偏移量；如果不存在，偏移量就是0L
      (0 until children).foreach { partitionId =>
        // 1-----------------------------start--------------------------1
        // 构建分区所属的Topic实例对象
        val tp = TopicAndPartition(topicName, partitionId)
        // 消费者针对Topic中某个分区在ZK Cluster上OFFSET的ZNODE路径
        val consumerOffsetDir = new ZKGroupTopicDirs(groupId, topicName).consumerOffsetDir
        val path: String = s"$consumerOffsetDir/$partitionId"

        // 判断是否存在，也就是判断某个分区的数据是否被消费
        if (zkClient.exists(path)) {
          //如果存在,则表示消费过此分区上的数据,直接获取OFFSET的值即可
          val offset = zkClient.readData[String](path).toLong
          //组合此分区复议的偏移量
          val tpo: (TopicAndPartition, Long) = tp -> offset
          //将分区对应 的偏移量组合的二元组放入Map集合中
          fromOffsets += tpo
        } else {
          //如果路径不存在,则表示未消费,偏移量设置为0L
          fromOffsets += tp -> 0
        }
        // 1-----------------------------end--------------------------1
      }
    }
    println(fromOffsets.mkString(", "))
    //d 返回不可变的map集合
    fromOffsets.toMap
  }

  /**
   * 将Topic中各个分区消费的偏移量保存至ZK中 ** @param offsetRange Topic分区偏移量
   *
   * @param groupId 消费组ID
   */
  def saveUtilOffsets(offsetRange: OffsetRange, groupId: String): Unit = {
    // a,构建分区与偏移量对象实例
    val tp = TopicAndPartition(offsetRange.topic, offsetRange.partition)
    //b 创建kafkaCluster实例对象
    val kafkaCluster = new KafkaCluster(
      Map("bootstrap.servers" -> ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
    )
    //c,更新zookeeper上消费偏移数据
    kafkaCluster.setConsumerOffsets(
      groupId, // 消费组ID
      Map(tp -> offsetRange.untilOffset)
    )
  } /**/
}
