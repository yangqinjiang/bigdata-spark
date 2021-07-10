import org.apache.spark.{SparkConf, SparkContext}

object SparkTopKey {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkTopKey").set("spark.testing.memory","471859200")
    val sc = new SparkContext(sparkConf)
    val wordCountRDD = sc.textFile("/datas/wordcount.data")
      .flatMap(line => line.split("\\s+"))
      .map(word=>(word,1))
      .reduceByKey((tmp,item) => tmp + item)
    wordCountRDD.foreach(println)

    // TODO: 按照词频count降序排序获取前3个单词, 有三种方式
    println("======================== sortByKey =========================")
    // 方式一：按照Key排序sortByKey函数， TODO： 建议使用sortByKey函数
    /*
    def sortByKey(
    ascending: Boolean = true,
    numPartitions: Int = self.partitions.length
    ): RDD[(K, V)]
    */
    wordCountRDD.map(tuple => tuple.swap) // 等效于 .map(tuple => (tuple._2,tuple._1))
        .sortByKey(ascending = true)
        .take(2)
        .foreach(println)


    println("======================== sortBy =========================")
    // 方式二：sortBy函数, 底层调用sortByKey函数
    /*
    def sortBy[K](
    f: (T) => K, // T 表示RDD集合中数据类型，此处为二元组
    ascending: Boolean = true,
    numPartitions: Int = this.partitions.length
    )
    (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
    */
    wordCountRDD.sortBy(tuple =>tuple._2,ascending = false).take(2).foreach(println)

    println("======================== top =========================")
    // 方式三：top函数，含义获取最大值，传递排序规则， TODO：慎用,当返回的结果数据集太大时,收集数据 到driver端排序
    /*
    def top(num: Int)(implicit ord: Ordering[T]): Array[T]
    */
    wordCountRDD.top(3)(Ordering.by(tuple => tuple._2)).foreach(println)
    // 为了测试，线程休眠，查看WEB UI界面
    Thread.sleep(1000000000)
    sc.stop()// TODO：应用程序运行接收，关闭资源
  }
}
