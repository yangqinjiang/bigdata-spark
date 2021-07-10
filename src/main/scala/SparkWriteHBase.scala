
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//将RDD数据保存至HBase表中
object SparkWriteHBase {


  def main(args: Array[String]): Unit = {
    // 构建SparkContext上下文实例对象
    val sc: SparkContext = {
      // a. 创建SparkConf对象，设置应用配置信息
      val sparkConf = new SparkConf().setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .set("spark.testing.memory", "471859200")

      // b. 创建SparkContext, 有就获取，没有就创建，建议使用
      val context = SparkContext.getOrCreate(sparkConf)
      context // 返回对象
    }
    sc.setLogLevel("WARN")

    //TODO:1 构建RDD
    val list = List(("hadoop", 123), ("spark", 345), ("hive", 567), ("ml", 789))
    val outputRDD: RDD[(String, Int)] = sc.parallelize(list, numSlices = 2)
    // TODO: 2、将数据写入到HBase表中, 使用saveAsNewAPIHadoopFile函数，要求RDD是(key, Value)
    // TODO: 组装RDD[(ImmutableBytesWritable, Put)]
    /**
     * 创建表: create 'htb_wordcount','info'
     * 查询表: scan 'htb_wordcount'
     * HBase表的设计：
     * 表的名称：htb_wordcount
     * Rowkey: word
     * 列簇: info
     * 字段名称： count
     */
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = outputRDD.mapPartitions { iter =>
      iter.map {
        case (word, count) =>
          //创建Put实例对象
          val put = new Put(Bytes.toBytes(word))
          //添加列
          put.addColumn(
            // 实际项目中使用HBase时,插入数据,先将所有字段的值转为String,再使用Bytes转换为字节数组
            Bytes.toBytes("info"), // 列族
            Bytes.toBytes("count"), //列名
            Bytes.toBytes(count.toString) //value
          )

          //返回二元组
          (new ImmutableBytesWritable(put.getRow), put)

      }
    }

    //构建HBase Client配置信息

    //构建HBase Client配置信息
    val conf: Configuration = HBaseConfiguration.create()
    //设置连接Zookeeper属性
    conf.set(HConstants.ZOOKEEPER_QUORUM, "hadoop102")
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")
    // 设置将数据保存的HBase表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_wordcount")
    /*
      def saveAsNewAPIHadoopFile(
      path: String,// 保存的路径
      keyClass: Class[_], // Key类型
      valueClass: Class[_], // Value类型
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]], // 输出格式OutputFormat实现
      conf: Configuration = self.context.hadoopConfiguration // 配置信息
      ): Unit
*/
    putsRDD.saveAsNewAPIHadoopFile(
      "datas/spark/htb-output-" + System.nanoTime(),
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )
    Thread.sleep(100000000)
    // 应用结束，关闭资源
    sc.stop()
  }
}
