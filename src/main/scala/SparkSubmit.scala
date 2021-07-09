import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**

 SPARK_HOME=/Users/yangqinjiang/software/spark-2.4.5-bin-hadoop2.7
 ${SPARK_HOME}/bin/spark-submit \
 --master 'local[2]' \
 --class SparkSubmit \
 hdfs://localhost:9000/spark/apps/bigdata-spark-1.0.0.jar \
 /datas/wordcount.data /datas/swc-output


 */
object SparkSubmit {

  def main(args: Array[String]): Unit = {

    // 为了程序健壮性，判断是否传递参数
    if(args.length != 2){
      println("Usage: SparkSubmit <input> <output> ........")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      //.setMaster("local[2]")
      .setAppName("SparkWordCount")

    val sc = new SparkContext(sparkConf)
    val wordCount: RDD[(String, Int)] = sc.textFile(args(0))
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))

      .reduceByKey((tmp, item) => tmp + item)
    wordCount.foreach(println)
    wordCount.saveAsTextFile(s"${args(1)}-${System.currentTimeMillis()}")


    sc.stop()// TODO：应用程序运行接收，关闭资源
  }
}
