package offline.report

import offline.utils.MySQLUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

/*

 */

/**
 * 报表开发：按照地域维度（省份和城市）分组统计广告被点击次数
 * 地域分布统计：region_stat_analysis
 */
object RegionStateReport extends MySQLUtils{


  /**
   * 不同业务报表统计分析时，两步骤：
   * i. 编写SQL或者DSL分析
   * ii. 将分析结果保存MySQL数据库表中
   */
  def doReport(dataframe: DataFrame) = {

    //导入隐式转换及函数 库

    import dataframe.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    // i,使用resultDF(调用DataFrame API)报表开发
    val resultDF: DataFrame = dataframe
      //按照地域维度分组(省份和城市)
      .groupBy($"province", $"city")
      // 直接count函数统计,列名为count
      .count()
      //按照次数进行降序排序
      .orderBy($"count".desc)
      //添加报表字段(报表统计的日期)
      .withColumn(
        "report_date", //报表日期字段
        //TODO:首先获取当前 日期,再减去1天获取昨天日期,转换为字符串类型
        date_sub(current_date(), 1).cast(StringType)
      )

    resultDF.printSchema()
    resultDF.show(10, truncate = false)
    //ii.保存分析报表结果到MySQL表中
    //将DataFrame转换为RDD操作,或者转换为Dataset操作

     resultDF.coalesce(1).rdd.foreachPartition(iter =>{
       val db_table = "itcast_ads_report.region_stat_analysis"
       val primaryKeyTupleSeq: Seq[(String, String)] = Seq(
         ("report_date","String"),
         ("province","String"),
         ("city","String"))

       val updateKeyTupleSeq: Seq[(String, String)] = Seq(
         ("count","Long"))

       saveToMySQL(iter,db_table,primaryKeyTupleSeq,updateKeyTupleSeq)
     })


  }

}
