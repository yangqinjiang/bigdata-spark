package rt.mock

import java.util.Properties

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import rt.config.ApplicationConfig

import scala.util.Random

/**
 * 实时产生交易订单数据，使用Json4J类库转换数据为JSON字符，发送Kafka Topic
 * 中
 *
 * kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic orderTopic --from-beginning
 */
object MockOrderProducer {

  def main(args: Array[String]): Unit = {
    var producer: KafkaProducer[String, String] = null
    try {
      // 1,kafka client producer配置信息
      val props = new Properties()
      props.put("bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)

      /**
       * ack=1，简单来说就是，producer只要收到一个分区副本成功写入的通知就认为推送消息成功了。这里有一个地方需要注意，这个副本必须是leader副本。只有leader副本成功写入了，producer才会认为消息发送成功。
       *
       * 注意，ack的默认值就是1。这个默认值其实就是吞吐量与可靠性的一个折中方案。生产上我们可以根据实际情况进行调整，比如如果你要追求高吞吐量，那么就要放弃可靠性。

       */
      props.put("acks", "1")//Kafka的ack机制，指的是producer的消息发送确认机制
      props.put("retries", "3")
      props.put("key.serializer", classOf[StringSerializer].getName)
      props.put("value.serializer", classOf[StringSerializer].getName)
      //创建KafkaProducer对象,传入配置信息
      producer = new KafkaProducer[String, String](props)
      // 随机数实例对象
      val random: Random = new Random()
      //订单状态: 订单打开0, 订单取消1,订单关闭2,订单完成3
      val allStatus = Array(0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      var i = 0
      while (true) {
        //每次循环,模拟产生的订单数目
        //每次订单数量
        val batchNumber: Int = random.nextInt(2) + 1
        (1 to batchNumber).foreach { number =>
          val currentTime: Long = System.currentTimeMillis()
          val orderId: String = s"${getDate(currentTime)}%06d".format(number)
          val userId: String = s"${1+random.nextInt(5)}%08d".format(random.nextInt(1000))
          val orderTime: String = getDate(currentTime, format = "yyyy-MM-dd HH:mm:ss.SSS")
          val orderMoney: String = s"${5 + random.nextInt(500)}.%02d".format(random.nextInt(100))
          val orderStatus: Int = allStatus(random.nextInt(allStatus.length))
          //3 订单记录数据
          val orderRecord = OrderRecord(orderId, userId, orderTime, getRandomIp, orderMoney.toDouble, orderStatus)
          //转换为json字符串
          val orderJson = new Json(DefaultFormats).write(orderRecord)
          i+=1
          if (0 == i % 20)println(".")else print (".")


          val record = new ProducerRecord[String, String](ApplicationConfig.KAFKA_SOURCE_TOPICS, orderJson)
          // 5. 发送数据：def send(messages: KeyedMessage[K,V]*), 将数据发送到Topic
          producer.send(record)
        }
        Thread.sleep(1000 + random.nextInt(1000))
      }
    }
  }

  def getDate(time: Long, format: String = "yyyyMMddHHmmssSSS"): String = {
    val fastFormat: FastDateFormat = FastDateFormat.getInstance(format)
    val formatDate: String = fastFormat.format(time) //格式化日期
    formatDate
  }

  /** ================= 获取随机IP地址 ================= */
  def getRandomIp: String = {
    // ip范围
    val range: Array[(Int, Int)] = Array(
      (607649792, 608174079), //36.56.0.0-36.63.255.255
      (1038614528, 1039007743), //61.232.0.0-61.237.255.255
      (1783627776, 1784676351), //106.80.0.0-106.95.255.255
      (2035023872, 2035154943), //121.76.0.0-121.77.255.255
      (2078801920, 2079064063), //123.232.0.0-123.235.255.255
      (-1950089216, -1948778497), //139.196.0.0-139.215.255.255
      (-1425539072, -1425014785), //171.8.0.0-171.15.255.255
      (-1236271104, -1235419137), //182.80.0.0-182.92.255.255
      (-770113536, -768606209), //210.25.0.0-210.47.255.255
      (-569376768, -564133889) //222.16.0.0-222.95.255.255
    )
    // 随机数：IP地址范围下标
    val random = new Random()
    val index = random.nextInt(10)
    val ipNumber: Int = range(index)._1 + random.nextInt(range(index)._2 - range(index)._1)
    // 转换Int类型IP地址为IPv4格式
    number2IpString(ipNumber)
  }

  /** =================将Int类型IPv4地址转换为字符串类型================= */
  def number2IpString(ip: Int): String = {
    val buffer: Array[Int] = new Array[Int](4)
    buffer(0) = (ip >> 24) & 0xff
    buffer(1) = (ip >> 16) & 0xff
    buffer(2) = (ip >> 8) & 0xff
    buffer(3) = ip & 0xff
    // 返回IPv4地址
    buffer.mkString(".")
  }

}
