import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import rt.config.ApplicationConfig

object Test {

  def main(args: Array[String]): Unit = {
//    import java.util
//    val terms:  util.List[Term] = HanLP.segment("中华共和国")
//    // 将java中集合对转换为scala中集合对象
//    import  collection.JavaConverters._
//    terms.asScala.map{term => (term.word,1)}.foreach(println)
//    import collection.mutable._
////    terms
//
//    val javaList: util.List[Int] = ArrayBuffer(1,2,3).asJava
//    println(javaList)

//    var holder = "?"
//    holder = "?" * 3
//    println(holder.split("").mkString(",").toString)

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      //设置shuffle分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory","471859200")
      .getOrCreate()

    //TODO: 导入隐式转换和函数库
    import spark.implicits._
    //    import org.apache.spark.sql.functions._

    // 从文件系统,监控目录,读取csv格式数据
    // 数据样本->   jack;23;running

    //定义schema
    val csvSchema: StructType = new StructType()
      .add("name", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)
      .add("hobby", StringType, nullable = true)

    val inputStreamDF: DataFrame = spark.readStream.option("sep", ";")
      .option("header", "false")
      .schema(csvSchema) // 指定schema信息
      .csv("file:///E:/csvdatas/")

    //依据业务需求,分析数据,统计年龄小于25岁的人群的爱好排行榜
    val resultStreamDF: Dataset[Row] = inputStreamDF
      //年龄小于25岁
      .filter($"age" < 25)
      .select("*")

    //设置streaming应用输出及启动
    val query = resultStreamDF.writeStream.outputMode(OutputMode.Append())
      .option("checkpointLocation", "datas/order-apps/ckpt/test-ckpt/")
      .format("es")
      .option("es.nodes", ApplicationConfig.ES_NODES)
      .option("es.port", ApplicationConfig.ES_PORT)
      .option("es.index.auto.create", ApplicationConfig.ES_INDEX_AUTO_CREATE)
      .option("es.write.operation", ApplicationConfig.ES_WRITE_OPERATION)
      .option("es.mapping.id", ApplicationConfig.ES_MAPPING_ID)
      .start(ApplicationConfig.ES_INDEX_NAME)
    // 查询器等待流式应用终止
    query.awaitTermination()
    query.stop() // 等待所有任务运行完成才停止运行
  }
}
