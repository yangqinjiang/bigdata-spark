package offline.report

import offline.config.ApplicationConfig
import offline.utils.MySQLUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 广告区域统计：ads_region_analysis，区域维度：省份和城市
 */
object AdsRegionAnalysisReport extends MySQLUtils{

  /**
   * 使用SQL方式计算广告投放报表
   */
  def reportWithKpiSql(dataframe: DataFrame): DataFrame = {

    //从DataFrame中获取SparkSession对象
    val spark: SparkSession = dataframe.sparkSession
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
   *
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
    reportDF.show(20, truncate = false)

    //iii.为了计算"三率"首先注册DataFrame为临时视图
    reportDF.createOrReplaceTempView("tmp_view_report")

    //iv,编写SQL并执行获取结果

    val resultDF: DataFrame = spark.sql(
      ReportSQLConstant.reportAdsRegionRateSQL("tmp_view_report")
    )

    resultDF.printSchema()
    resultDF.show(20, truncate = false)

    //返回结果
    resultDF
  }

  /**
   * 保存数据至MySQL表中，直接使用DataFrame Writer操作，但是不符合实际应用需求
   */
  def saveResultToMySQL(dataframe: DataFrame) = {
    dataframe.coalesce(1)
      .write.mode(SaveMode.Append) // 设置MySQL数据库相关属性
      .format("jdbc")
      .option("driver", ApplicationConfig.MYSQL_JDBC_DRIVER)
      .option("url", ApplicationConfig.MYSQL_JDBC_URL)
      .option("user", ApplicationConfig.MYSQL_JDBC_USERNAME)
      .option("password", ApplicationConfig.MYSQL_JDBC_PASSWORD)
      .option("dbtable", "itcast_ads_report.ads_region_analysis")
      .save()
  }

  /**
   * 使用DSL方式计算广告投放报表
   */
  def reportWithDsl(dataframe: DataFrame): DataFrame = {
    // i. 导入隐式转换及函数库

    import dataframe.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    // ii 报表开发
    var reportDF: DataFrame = dataframe
      //第一步,按照维度分组,省份和城市
      .groupBy($"province", $"city")
      //第二步,使用agg进行聚合操作,主要使用CASE...WHEN...函数和SUM函数
      .agg(
        //原始请求: requestmode = 1 and processnode >= 1
        sum(
          when($"requestmode".equalTo(1).and($"processnode".geq(1)), 1).otherwise(0)
        ).as("orginal_req_cnt"),
        // 有效请求：requestmode = 1 and processnode >= 2
        sum(
          when($"requestmode".equalTo(1)
            .and($"processnode".geq(2)), 1
          ).otherwise(0)
        ).as("valid_req_cnt"),
        // 广告请求：requestmode = 1 and processnode = 3
        sum(
          when($"requestmode".equalTo(1)
            .and($"processnode".equalTo(3)), 1
          ).otherwise(0)
        ).as("ad_req_cnt"),
        // 参与竞价数
        sum(
          when($"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"isbid".equalTo(1))
            .and($"adorderid".notEqual(0)), 1
          ).otherwise(0)
        ).as("join_rtx_cnt"),
        // 竞价成功数
        sum(
          when($"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"iswin".equalTo(1))
            .and($"adorderid".notEqual(0)), 1
          ).otherwise(0)
        ).as("success_rtx_cnt"),
        // 广告主展示数: requestmode = 2 and iseffective = 1
        sum(
          when($"requestmode".equalTo(2)
            .and($"iseffective".equalTo(1)), 1
          ).otherwise(0)
        ).as("ad_show_cnt"),
        // 广告主点击数: requestmode = 3 and iseffective = 1 and adorderid != 0
        sum(
          when($"requestmode".equalTo(3)
            .and($"iseffective".equalTo(1))
            .and($"adorderid".notEqual(0)), 1
          ).otherwise(0)
        ).as("ad_click_cnt"),
        // 媒介展示数
        sum(
          when($"requestmode".equalTo(2)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"isbid".equalTo(1))
            .and($"iswin".equalTo(1)), 1
          ).otherwise(0)
        ).as("media_show_cnt"),
        // 媒介点击数
        sum(
          when($"requestmode".equalTo(3)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"isbid".equalTo(1))
            .and($"iswin".equalTo(1)), 1
          ).otherwise(0)
        ).as("media_click_cnt"),
        // DSP 广告消费
        sum(
          when($"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"iswin".equalTo(1))
            .and($"adorderid".gt(200000))
            .and($"adcreativeid".gt(200000)), floor($"winprice" / 1000)) otherwise (0)
        ).as("dsp_pay_money"),
        // DSP广告成本
        sum(
          when($"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"isbid".equalTo(1))
            .and($"iswin".equalTo(1))
            .and($"adorderid".gt(200000))
            .and($"adcreativeid".gt(200000)), floor($"adpayment" / 1000)) otherwise (0)
        ).as("dsp_cost_money"))
      // 第三步、过滤非0数据
      .filter(
        $"join_rtx_cnt".notEqual(0)
          .and($"success_rtx_cnt".notEqual(0))
          .and($"ad_show_cnt".notEqual(0))
          .and($"ad_click_cnt".notEqual(0))
          .and($"media_show_cnt".notEqual(0))
          .and($"media_click_cnt".notEqual(0))
      )
      // 第四步、计算“三率”, 增加三列数据
      .withColumn(
        "success_rtx_rate", //
        round($"success_rtx_cnt" / $"join_rtx_cnt", 2) // 保留两位有效数字
      )
      .withColumn(
        "ad_click_rate", //
        round($"ad_click_cnt" / $"ad_show_cnt", 2) // 保留两位有效数字
      )
      .withColumn(
        "media_click_rate", //
        round($"media_click_cnt" / $"media_show_cnt", 2) // 保留两位有效数字
      )
      // 第五步、增加报表的日期
      .withColumn(
        "report_date", // 报表日期字段
        date_sub(current_date(), 1).cast(StringType)
      )
    // iii. 返回结果数据
    reportDF
    
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
    //    val resultDF:DataFrame = reportWithKpiSql(dataframe)//sql编程
    val resultDF: DataFrame = reportWithDsl(dataframe) //dsl编程

    // 第二,保存数据
//    resultDF.show(20, truncate = false)
    resultDF.select("report_date","province","city").show(2)

    resultDF.coalesce(1).rdd.foreachPartition(iter =>{

      val db_table = "itcast_ads_report.ads_region_analysis"
      val primaryKeyTupleSeq: Seq[(String, String)] = Seq(
        ("report_date","String"),
        ("province","String"),
        ("city","String"))
      val updateKeyTupleSeq: Seq[(String, String)] = Seq(
        ("orginal_req_cnt","Long"),
        ("valid_req_cnt","Long"),
        ("ad_req_cnt","Long"),
        ("join_rtx_cnt","Long"),
        ("success_rtx_cnt","Long"),
        ("ad_show_cnt","Long"),
        ("ad_click_cnt","Long"),
        ("media_show_cnt","Long"),
        ("media_click_cnt","Long"),
        ("dsp_pay_money","Long"),
        ("dsp_cost_money","Long"),
        ("success_rtx_rate","double"),
        ("ad_click_rate","double"),
        ("media_click_rate","double"))
      saveToMySQL(iter,db_table,primaryKeyTupleSeq,updateKeyTupleSeq)
    })
  }
}
