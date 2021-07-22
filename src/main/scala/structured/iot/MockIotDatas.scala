package structured.iot

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.util.Random
//相当于大机房中各个服务器定时发送相关监控数据至Kafka中，服务器部署服务有数据库db、大
//数据集群bigdata、消息队列kafka及路由器route等等，数据样本
//{"device":"device_50","deviceType":"bigdata","signal":91.0,"time":1590660338429}
//{"device":"device_20","deviceType":"bigdata","signal":17.0,"time":1590660338790}
object MockIotDatas {

  def main(args: Array[String]): Unit = {
    //发送kafka topic
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    //kafka生产者
    val producer = new KafkaProducer[String, String](props)

    val random = new Random()
    val deviceTypes = Array(
      "db","bigdata","kafka","route","bigdata","db","bigdata","bigdata","bigdata"
    )

    while (true) {

      val index: Int = random.nextInt(deviceTypes.length)
      val deviceId = s"device_${(index + 1) * 10 + random.nextInt(index + 1)}"
      val deviceType: String = deviceTypes(index)
      val deviceSignal = 10 + random.nextInt(90)
      val deviceData = DeviceData(deviceId, deviceType, deviceSignal, System.currentTimeMillis())
      //转换为json字符串
      val deviceJson = new Json(DefaultFormats).write(deviceData)
      println(deviceJson)

      val record = new ProducerRecord[String, String]("iotTopic", deviceJson)
      producer.send(record)
      Thread.sleep(1000 + random.nextInt(1000))
    }
    producer.close() //关闭连接
  }
}
