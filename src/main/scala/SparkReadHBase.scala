
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//从HBase 表中读取数据，封装到RDD数据集
object SparkReadHBase {


  def main(args: Array[String]): Unit = {
    // 构建SparkContext上下文实例对象
    val sc: SparkContext = {
      // a. 创建SparkConf对象，设置应用配置信息
      val sparkConf = new SparkConf().setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .set("spark.testing.memory", "471859200")
        //TODO: 设置使用Kryo序列化方式
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //TODO: 注册序列化的数据类型
        .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))

      // b. 创建SparkContext, 有就获取，没有就创建，建议使用
      val context = SparkContext.getOrCreate(sparkConf)
      context // 返回对象
    }
    sc.setLogLevel("WARN")


    //构建HBase Client配置信息

    //构建HBase Client配置信息
    val conf: Configuration = HBaseConfiguration.create()
    //设置连接Zookeeper属性
    conf.set(HConstants.ZOOKEEPER_QUORUM, "hadoop102")
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")
    // 设置将数据保存的HBase表的名称
    conf.set(TableInputFormat.INPUT_TABLE, "htb_wordcount")
    /*
    def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
        conf: Configuration = hadoopConfiguration,
        fClass: Class[F],
        kClass: Class[K],
        vClass: Class[V]
        ): RDD[(K, V)]
    */
    //RDD
    val resultRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf, //
      classOf[TableInputFormat], //
      classOf[ImmutableBytesWritable], //
      classOf[Result]
    )
    println(s"Count = ${resultRDD.count()}")
    //java.io.NotSerializableException: org.apache.hadoop.hbase.io.ImmutableBytesWritable
    //分析: 表示ImmutableBytesWritable不能被序列化
    // 解决:设置序列化为Kryo方式即可
    resultRDD.take(5).foreach {
      case (rowKey, result) =>
        print(s"RowKey = ${Bytes.toString(rowKey.get())}\t\t")
        result.rawCells().foreach{
          cell =>
            val cf = Bytes.toString(CellUtil.cloneFamily(cell))
            val column = Bytes.toString(CellUtil.cloneQualifier(cell))
            val value = Bytes.toString(CellUtil.cloneValue(cell))
            val version = cell.getTimestamp
            println(s"\t column = $cf:$column , version = $version , value = $value")
        }
    }
    //    Thread.sleep(100000000)
    // 应用结束，关闭资源
    sc.stop()
  }
}
