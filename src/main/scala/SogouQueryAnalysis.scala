import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 用户查询日志(SogouQ)分析，数据来源Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。
 * 1. 搜索关键词统计，使用HanLP中文分词
 * 2. 用户搜索次数统计
 * 3. 搜索时间段统计
 * 数据格式：
 * 访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
 * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对
 * 应同一个用户ID
 */
object SogouQueryAnalysis {

  def main(args: Array[String]): Unit = {

    // 构建SparkContext上下文实例对象
    val sc: SparkContext = {
      // a. 创建SparkConf对象，设置应用配置信息
      val sparkConf = new SparkConf().setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .set("spark.testing.memory","471859200")

      // b. 创建SparkContext, 有就获取，没有就创建，建议使用
      val context = SparkContext.getOrCreate(sparkConf)
      context // 返回对象
    }

    sc.setLogLevel("WARN")
    // TODO: 1本地读取 SogouQ用户查询日志数据
    //val rawLogsRDD: RDD[String] = sc.textFile("/datas/sogou/SogouQ.sample")
    val rawLogsRDD: RDD[String] = sc.textFile("/datas/sogou/SogouQ.reduced")
    println(s"Count = ${rawLogsRDD.count()}")

    //TODO 2.解析数据 ,封装到CaseClass样例类中
    val recordsRDD: RDD[SogouRecord] = rawLogsRDD
      // 过滤不合法数据，如null，分割后长度不等于6
      .filter(log => null != log && log.trim.split("\\s+").length == 6)
      //// 对每个分区中数据进行解析，封装到SogouRecord
      .mapPartitions {
        iter =>
          iter.map {
            log => {
              //拼装SogouRecord
              val arr = log.trim.split("\\s+")
              SogouRecord(
                arr(0), arr(1), arr(2).replaceAll("\\[|\\]", ""), //
                arr(3).toInt, arr(4).toInt, arr(5) //
              )
            }
          }
      }

    //


    //数据使用多次，进行缓存操作，使用count触发
    recordsRDD.persist(StorageLevel.MEMORY_AND_DISK).count()
    println(s"Count = ${recordsRDD.count()} ,First = ${recordsRDD.first()}")
    // TODO: 3. 依据需求统计分析
    /*
    1. 搜索关键词统计，使用HanLP中文分词
    2. 用户搜索次数统计
    3. 搜索时间段统计
    */
    // =================== 3.1 搜索关键词统计 ===================
    // a. 获取搜索词，进行中文分词
    val wordsRDD: RDD[(String, Int)] = recordsRDD.flatMap { record =>
//      println(record.queryWords.trim)
      val queryWords = record.queryWords.trim
      // 使用HanLP中文分词库进行分词
      import java.util
      val terms: util.List[Term] = HanLP.segment(queryWords)
      //      println(terms)
      // 将java中集合对转换为scala中集合对象
      import collection.JavaConverters._
      terms.asScala.map { term => (term.word, 1) }

    }

    println(s"Count = ${wordsRDD.count()}, Example = ${wordsRDD.take(5).mkString(",")}")

    // b. 统计搜索词出现次数，获取次数最多Top10
    wordsRDD.map(word => (word, 1)) // 每个单词出现一次
      .reduceByKey((tmp, item) => tmp + item) //分组统计次数
      .map(tuple => tuple.swap)
      .sortByKey(ascending = false) // 词频降序排序
      .take(10) // 获取前10个搜索词
      .foreach(println)

    // =================== 3.2 用户搜索点击次数统计 ===================
    /*
    每个用户在搜索引擎输入关键词以后，统计点击网页数目，反应搜索引擎准确度
    先按照用户ID分组，再按照搜索词分组，统计出每个用户每个搜索词点击网页个数
    */
    val clickCountRDD: RDD[((String, String), Int)] = recordsRDD.map { record =>
      //获取用户ID和搜索词
      val key = record.userId -> record.queryWords
      (key, 1)
    }
      //按照用户ID和搜索词组合的Key分组聚合
      .reduceByKey((tmp, item) => tmp + item)
    clickCountRDD.sortBy(tuple => tuple._2, ascending = false)
      .take(10).foreach(println)
    println(s"Max Click Count = ${clickCountRDD.map(_._2).max()}")
    println(s"Min Click Count = ${clickCountRDD.map(_._2).min()}")
    println(s"Avg Click Count = ${clickCountRDD.map(_._2).mean()}")


    // =================== 3.3 搜索时间段统计 ===================
    /*
    从搜索时间字段获取小时，统计个小时搜索次数
    */
    val hourSearchRDD: RDD[(String, Int)] = recordsRDD.map { record =>
      //提取小时, 03:12:50
      record.queryTime.substring(0, 2)
    }
      //分组聚合
      .map(word => (word, 1)) //每个单词出现一次
      .reduceByKey((tmp, item) => tmp + item) //分组统计次数
      .sortBy(tuple => tuple._2, ascending = false)
    hourSearchRDD.foreach(println)

    recordsRDD.unpersist() //释放缓存数据
    Thread.sleep(100000000)
    // 应用结束，关闭资源
    sc.stop()


  }
}
