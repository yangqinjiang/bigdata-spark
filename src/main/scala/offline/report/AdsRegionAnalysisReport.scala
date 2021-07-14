package offline.report

import java.sql.{Connection, DriverManager, PreparedStatement}

import offline.config.ApplicationConfig
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
 * 广告区域统计：ads_region_analysis，区域维度：省份和城市
 */
object AdsRegionAnalysisReport {

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
   * 保存数据至MySQL数据库，使用函数foreachPartition对每个分区数据操作，主键存在时更新，不存在时插入
   *
   * @param iter
   */
  def saveToMySQL(iter: Iterator[Row]): Unit = {
    //加载驱动类
    Class.forName(ApplicationConfig.MYSQL_JDBC_DRIVER)
    //声明变量
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try {
      //b获取连接
      conn =  DriverManager.getConnection(
        ApplicationConfig.MYSQL_JDBC_URL, //
        ApplicationConfig.MYSQL_JDBC_USERNAME, //
        ApplicationConfig.MYSQL_JDBC_PASSWORD
      )
      //c 获取preparedStatement对象
      val insertSql =
        """
          |INSERT INTO
          | itcast_ads_report.ads_region_analysis
          |  (report_date,province,city,orginal_req_cnt,valid_req_cnt,ad_req_cnt,join_rtx_cnt,success_rtx_cnt,ad_show_cnt,ad_click_cnt,media_show_cnt,media_click_cnt,dsp_pay_money,dsp_cost_money,success_rtx_rate,ad_click_rate,media_click_rate)
          | VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          |  ON DUPLICATE KEY UPDATE
          |     orginal_req_cnt=VALUES(orginal_req_cnt),
          |     valid_req_cnt=VALUES(valid_req_cnt),
          |     ad_req_cnt=VALUES(ad_req_cnt),
          |     join_rtx_cnt=VALUES(join_rtx_cnt),
          |     success_rtx_cnt=VALUES(success_rtx_cnt),
          |     ad_show_cnt=VALUES(ad_show_cnt),
          |     ad_click_cnt=VALUES(ad_click_cnt),
          |     media_show_cnt=VALUES(media_show_cnt),
          |     media_click_cnt=VALUES(media_click_cnt),
          |     dsp_pay_money=VALUES(dsp_pay_money),
          |     dsp_cost_money=VALUES(dsp_cost_money),
          |     success_rtx_rate=VALUES(success_rtx_rate),
          |     ad_click_rate=VALUES(ad_click_rate),
          |     media_click_rate=VALUES(media_click_rate)
          |""".stripMargin

      pstmt = conn.prepareStatement(insertSql)
      val beforeCommitStatus = conn.getAutoCommit //保存一下原来的自动提交状态
      // 不自动提交
      conn.setAutoCommit(false)
      // 将分区中数据插入到表中,批量插入
      // 采用批量插入的方式将RDD分区数据插入到MySQL表中，提升性能
      iter.foreach { row =>
        pstmt.setString(1, row.getAs[String]("report_date"))
        pstmt.setString(2, row.getAs[String]("province"))
        pstmt.setString(3, row.getAs[String]("city"))
        pstmt.setLong(4, row.getAs[Long]("orginal_req_cnt"))
        pstmt.setLong(5, row.getAs[Long]("valid_req_cnt"))
        pstmt.setLong(6, row.getAs[Long]("ad_req_cnt"))
        pstmt.setLong(7, row.getAs[Long]("join_rtx_cnt"))
        pstmt.setLong(8, row.getAs[Long]("success_rtx_cnt"))
        pstmt.setLong(9, row.getAs[Long]("ad_show_cnt"))
        pstmt.setLong(10, row.getAs[Long]("ad_click_cnt"))
        pstmt.setLong(11, row.getAs[Long]("media_show_cnt"))
        pstmt.setLong(12, row.getAs[Long]("media_click_cnt"))
        pstmt.setLong(13, row.getAs[Long]("dsp_pay_money"))
        pstmt.setLong(14, row.getAs[Long]("dsp_cost_money"))
        //三率
        pstmt.setDouble(15, row.getAs[Double]("success_rtx_rate"))
        pstmt.setDouble(16, row.getAs[Double]("ad_click_rate"))
        pstmt.setDouble(17, row.getAs[Double]("media_click_rate"))

        //加入批次
        pstmt.addBatch()

      }
      //批量插入
      pstmt.executeBatch()
      conn.commit()
      conn.setAutoCommit(beforeCommitStatus) // 恢复原来的自动提交状态


    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != pstmt) pstmt.close()
      if (null != conn) conn.close()
    }
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
//    saveResultToMySQL(resultDF)
    resultDF.coalesce(1).rdd.foreachPartition(iter => saveToMySQL(iter))
  }
}
