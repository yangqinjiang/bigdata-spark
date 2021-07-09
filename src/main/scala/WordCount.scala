import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkWordCount")

    val sc = new SparkContext(sparkConf)
    val wordCount: RDD[(String, Int)] = sc.textFile("/datas/wordcount.data")
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((tmp, item) => tmp + item)
    wordCount.foreach(println)
    wordCount.saveAsTextFile(s"/datas/swc-output-${System.currentTimeMillis()}")


    // 为了测试，线程休眠，查看WEB UI界面
    Thread.sleep(1000000000)
    sc.stop()// TODO：应用程序运行接收，关闭资源
  }
}
