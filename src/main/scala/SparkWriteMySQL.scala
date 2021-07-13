import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteMySQL {

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

    //1 从HDFS读取文本数据,封装集合RDD
    val inputRDD: RDD[String] = sc.textFile("/datas/wordcount.data")
    //2 处理数据,调用RDD中函数
    val resultRDD: RDD[(String, Int)] = inputRDD
      // 3.a每行数据分割为单词
      .flatMap(line => line.split("\\s+"))
      //3.b转换为二元组,表示每个单词出现一次
      .map(word => (word, 1))
      //3.c 按照Key分组聚合
      .reduceByKey((tmp, item) => tmp + item)

    resultRDD.foreach(println)
    //3 输出结果RDD保存到MySQL数据库
    resultRDD
      //对结果RDD保存到外部存储系统时,考虑降低RDD分区数目
      .coalesce(1)
      //对分区数据操作
      .foreachPartition { iter => saveToMySQL(iter) }

    sc.stop()


  }

  /**
   * 将每个分区中的数据保存到MySQL表中
   *
   * @param datas 迭代器，封装RDD中每个分区的数据
   */
  def saveToMySQL(datas: Iterator[(String, Int)]): Unit = {

    //a,加载驱动类
    Class.forName("com.mysql.cj.jdbc.Driver")
    //声明变量
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try {
      // b, 获取连接
      conn = DriverManager.getConnection(
        "jdbc:mysql://hadoop102:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
        "root",
        "123456"
      )
      //c 获取PreparedStatement对象
      //TODO:会发生 Duplicate Key错误
      //val insertSql = "INSERT INTO db_test.tb_wordcount(word,count) VALUES(?,?)"

      //如果word已存在,则更新count值
      val insertSql = "INSERT INTO db_test.tb_wordcount(word,count) VALUES(?,?) ON DUPLICATE KEY UPDATE `count` = VALUES(count)"
      pstmt = conn.prepareStatement(insertSql)
      conn.setAutoCommit(false)

      //d将分区中数据插入到表中,批量插入
      datas.foreach { case (word, count) =>
        pstmt.setString(1, word)
        pstmt.setLong(2, count.toLong)
        //加入批次
        pstmt.addBatch()
      }
      //TODO: 批量插入
      pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != pstmt) pstmt.close()
      if (null != conn) conn.close()
    }
  }
}
