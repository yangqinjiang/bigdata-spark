package offline.report

import offline.config.ApplicationConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 广告区域统计：ads_region_analysis，区域维度：省份和城市
 */
object AdsRegionAnalysisReport {

  /**
   * 使用SQL方式计算广告投放报表
   */
  def reportWithKpiSql(dataframe: DataFrame): DataFrame = {

    //从DataFrame中获取SparkSession对象
    val spark:SparkSession = dataframe.sparkSession
    /*
    在SparkSQL中使用SQL分析数据时，步骤分为两步：
    - 第一步、将DataFrame注册为临时视图
    - 第二步、编写SQL语句，使用SparkSession执行
    */
    // i. 注册广告数据集为临时视图：tmp_view_pmt
    dataframe.createOrReplaceTempView("tmp_view_pmt")
    // ii. 编写SQL并执行获取结果
    val kpiSql: String = ReportSQLConstant.reportAdsRegionKpiSQL("tmp_view_pmt")
    //println(kpiSql)
    val reportDF: DataFrame = spark.sql(kpiSql)
    //reportDF.show(20, truncate = false)
    // iii. 返回结果
    reportDF
  }

  /**
   * 使用SQL 方式计算广告投放报表
   * @param dataframe
   * @return
   */
  def reportWithSql(dataframe: DataFrame): DataFrame = {
    //从DataFrame中获取SparkSession对象
    val spark = dataframe.sparkSession

    /*
    在SparkSQL中使用SQL分析数据时，步骤分为两步：
    - 第一步、将DataFrame注册为临时视图
    - 第二步、编写SQL语句，使用SparkSession执行
    */
    val tmp_view_pmt = "tmp_view_pmt"
    //i, 注册广告数据集为临时视图,tmp_view_pmt
    dataframe.createOrReplaceTempView(tmp_view_pmt)
    //ii, 编写sql并执行获取结果
    val reportDF: DataFrame = spark.sql(
      ReportSQLConstant.reportAdsRegionSQL(tmp_view_pmt)
    )
    reportDF.printSchema()
    reportDF.show(20,truncate = false)

    //iii.为了计算"三率"首先注册DataFrame为临时视图
    reportDF.createOrReplaceTempView("tmp_view_report")

    //iv,编写SQL并执行获取结果

    val resultDF: DataFrame = spark.sql(
      ReportSQLConstant.reportAdsRegionRateSQL("tmp_view_report")
    )

    resultDF.printSchema()
    resultDF.show(20,truncate = false)

    //返回结果
    resultDF
  }

  /**
   * 保存数据至MySQL表中，直接使用DataFrame Writer操作，但是不符合实际应用需求
   */
  def saveResultToMySQL(dataframe: DataFrame) = {
    dataframe.coalesce(1)
      .write.mode(SaveMode.Append)// 设置MySQL数据库相关属性
      .format("jdbc")
      .option("driver", ApplicationConfig.MYSQL_JDBC_DRIVER)
      .option("url", ApplicationConfig.MYSQL_JDBC_URL)
      .option("user", ApplicationConfig.MYSQL_JDBC_USERNAME)
      .option("password", ApplicationConfig.MYSQL_JDBC_PASSWORD)
      .option("dbtable", "itcast_ads_report.ads_region_analysis")
      .save()
  }

  /*
        不同业务报表统计分析时，两步骤：
        i. 编写SQL或者DSL分析
        ii. 将分析结果保存MySQL数据库表中


        */
  def doReport(dataframe: DataFrame) = {

    //第一,计算报表
   //上述SQL使用子查询方式，需要两次注册DataFrame为临时视图，编写SQL语句，可以使用With As语句优化。
//    val resultDF:DataFrame = reportWithSql(dataframe)//sql编程
    val resultDF:DataFrame = reportWithKpiSql(dataframe)//sql编程
//    val resultDF:DataFrame = reportWithDql(dataframe)//dsl编程

    // 第二,保存数据
    resultDF.show(20,truncate = false)
    saveResultToMySQL(resultDF)
  }
}
