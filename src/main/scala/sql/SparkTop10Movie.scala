package sql

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
/**
 * 需求：对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
 */
////设置运行参数 vm args:  -DHADOOP_USER_NAME=atguigu
object SparkTop10Movie {

  def main(args: Array[String]): Unit = {
   val spark =  SparkSession.builder()
      .master("local[4]")
     .config("spark.testing.memory", "471859200") //
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
    //todo: 设置shuffle时分区数目,默认值是200
      .config("spark.sql.shuffle.partitions","4")
      .getOrCreate()



    //在这里加上HADOOP_USER_NAME用户配置,解决报错问题。
//    System.getProperties.setProperty("HADOOP_USER_NAME", "root")

    //导入隐式转换
    import spark.implicits._
    // 1. 读取电影评分数据，从本地文件系统读取
val rawRatingsDS = spark.read.textFile("/datas/ml-1m/ratings.dat")
    val ratingsDF = rawRatingsDS.filter(line => null != line && line.trim.split("::").length == 4)
      .mapPartitions { iter =>
        iter.map { line =>
          //按照分割符分割,拆箱到变量中
          val Array(userId, movieId, rating, timestamp) = line.trim.split("::")
          //返回四元组
          (userId, movieId, rating, timestamp)
        }
      }
      //指定列名添加schema
      .toDF("userId", "movieId", "rating", "timestamp")

    ratingsDF.printSchema()

    //TODO: 基于SQL方式分析
    //第一步,注册Dataframe为临时视图
    ratingsDF.createOrReplaceTempView("view_temp_ratings")
    //第二步,编写SQL
    val top10MovieDF = spark.sql(
      """
        |SELECT
        | movieId,ROUND(AVG(rating),2) AS avg_rating,COUNT(movieId) AS cnt_rating
        |FROM
        | view_temp_ratings
        |GROUP BY
        | movieId
        |HAVING
        |cnt_rating > 2000
        |ORDER BY
        | avg_rating DESC,cnt_rating DESC
        |LIMIT
        | 10
        |""".stripMargin)

    top10MovieDF.printSchema()
    top10MovieDF.show(10,truncate = false)
    println("=====================================")
    import org.apache.spark.sql.functions._
    val resultDF = ratingsDF
      //选取字段
      .select($"movieId", $"rating")
      //分组,按照电影ID,获取平均评分和评分次数
      .groupBy($"movieId")
      .agg(
        round(avg($"rating"), 2).as("avg_rating"), //
        count($"movieId").as("cnt_rating") //
      )
      //过滤,评分次数大于2000
      .filter($"cnt_rating" > 2000)
      //排序,先按评分降序,再按照次数降序
      .orderBy($"avg_rating".desc, $"cnt_rating".desc)
      //获取前10
      .limit(10)

    resultDF.printSchema()
    resultDF.show(10)

    //TODO:将分析的结果数据保存到MySQL数据库和csv文件
    // 结果DataFrame被使用多次,缓存
    resultDF.persist(StorageLevel.MEMORY_AND_DISK)
    //保存到MySQL数据库表汇总
    resultDF
        .coalesce(1)//考虑降低分区数目
        .write
        .mode("overwrite")
        .option("driver","com.mysql.cj.jdbc.Driver")
        .option("user","root")
        .option("password","123456")
        .jdbc(
          "jdbc:mysql://hadoop102:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
          "db_test.tb_top10_movies",
          new Properties()
        )
    //2 保存csv文件,每行数据中字段之间使用逗号隔开
    //设置运行参数 vm args:  -DHADOOP_USER_NAME=atguigu
    resultDF.coalesce(1)
      .write
      .mode("overwrite")
        .csv("/datas/top10-movies2")


    //释放缓存数据
    resultDF.unpersist()
    Thread.sleep(10000000)
    spark.stop()
  }
}
