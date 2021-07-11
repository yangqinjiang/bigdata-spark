package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkRDDInferring {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .config("spark.testing.memory", "471859200") //
      .getOrCreate()
    // 读取电影评分数据u.data, 每行数据有四个字段，使用制表符分割
    // user id | item id | rating | timestamp
    val rawRatingsRDD: RDD[String] = spark.sparkContext.textFile("/datas/ml-100k/u.data", minPartitions = 2)
    val ratingsRDD = rawRatingsRDD.filter(line => null != line && line.trim.split("\t").length == 4)
      .mapPartitions {
        iter =>
          iter.map {
            line =>
              //拆箱操作,python中常用
              val Array(userId, itemId, rating, timestamp) = line.trim.split("\t")
              // 返回MovieRating实例对象
              MovieRating(userId, itemId, rating.toDouble, timestamp.toLong)
          }
      }
    //将RDD转换为DataFrame和Dataset
    //此种方式要求RDD数据类型必须为CaseClass，转换的DataFrame中字段名称就是CaseClass中
    //属性名称
    import spark.implicits._
    val ratingsDF: DataFrame = ratingsRDD.toDF

    ratingsDF.printSchema()
    ratingsDF.show(10)

    spark.stop()

  }
}
