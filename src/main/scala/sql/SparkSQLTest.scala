package sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLTest {
  def main(args: Array[String]): Unit = {
    // 构建 SparkSession 实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory", "471859200")
      .getOrCreate()

    import spark.implicits._
    val dataframe: DataFrame = Seq(
      ("spark", 3), ("hadoop", 1), ("hive", 2)
    ).toDF("value", "count")
    dataframe.show(10, truncate = false)
    // TODO: 行转列
    val df = dataframe
      .groupBy()
      .pivot($"value")
      .sum("count")//将count字段拿出来
    // df.printSchema()
    df.show(10, truncate = false)
    spark.stop()
  }
}
