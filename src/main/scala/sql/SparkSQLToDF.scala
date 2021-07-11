package sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 隐式调用toDF函数，将数据类型为元组的Seq和RDD集合转换为DataFrame
 */
object SparkSQLToDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .config("spark.testing.memory", "471859200") //
      .getOrCreate()

    import spark.implicits._
   // 1构建RDD,数据类型为三元组形式
    val usersRDD =spark.sparkContext.parallelize(
      Seq(
        (10001,"zhangsan",23),
        (10002, "lisi", 22),
        (10003, "wangwu", 23),
        (10004, "zhaoliu", 24)
      )
    )
    //将RDD转换为DataFrame
    val usersDF = usersRDD.toDF("id", "name", "age")
    usersDF.printSchema()
    usersDF.show(10,truncate = false)
    println("====================")

    // scala 列表, 隐式转换
    val df: DataFrame = Seq(
      (10001, "zhangsan", 23),
      (10002, "lisi", 22),
      (10003, "wangwu", 23),
      (10004, "zhaoliu", 24)
    ).toDF("id", "name", "age")
    df.printSchema()
    df.show(10,truncate = false)
    spark.stop()

  }
}
