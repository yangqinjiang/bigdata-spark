package sql

import org.apache.spark.sql.SparkSession

////设置运行参数 vm args:  -DHADOOP_USER_NAME=atguigu
object SparkSQLHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .config("spark.testing.memory", "471859200") //
      //指定hive metastore服务地址
      .config("hive.metastore.uris", "thrift://hadoop102:9083")
      //表示集成hive,读取hive表的数据
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("SELECT name,round(avg(score),2) as avg_score FROM default.score group by name").show()

    println("==============================")
    // 导入隐式转换
    import spark.implicits._
    // 导入函数库
    import org.apache.spark.sql.functions._
    //设置运行参数 vm args:  -DHADOOP_USER_NAME=atguigu
    spark.read.table("default.score")
      .groupBy($"name")
      .agg(round(avg($"score"), 2).alias("avg_score"))
      .show(10, truncate = false)
    spark.stop()
  }
}
