package offline.report

import java.sql.{Connection, PreparedStatement, DriverManager}

import offline.config.ApplicationConfig
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/*
-- 创建数据库，不存在时创建
-- DROP DATABASE IF EXISTS itcast_ads_report;
CREATE DATABASE IF NOT EXISTS itcast_ads_report;
USE itcast_ads_report;
-- 创建表
-- DROP TABLE IF EXISTS itcast_ads_report.region_stat_analysis ;
CREATE TABLE `itcast_ads_report`.`region_stat_analysis` (
`report_date` varchar(255) NOT NULL,
`province` varchar(255) NOT NULL,
`city` varchar(255) NOT NULL,
`count` bigint DEFAULT NULL,
PRIMARY KEY (`report_date`,`province`,`city`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



 */

/**
 * 报表开发：按照地域维度（省份和城市）分组统计广告被点击次数
 * 地域分布统计：region_stat_analysis
 */
object RegionStateReport {

  /**
   * 保存数据至MySQL表中，直接使用DataFrame Writer操作，但是不符合实际应用需求
   */
  def saveResultToMySQL(dataframe: DataFrame) = {
    dataframe.coalesce(1)
      .write
      // Overwrite表示，当表存在时，先删除表，再创建表和插入数据, 所以不用此种方式
      //      .mode(SaveMode.Overwrite)
      //TODO;当多次运行程序时, 比如对某日广告数据报表分析运行两次,由于报表结果主键存在数据库表中,产生冲突,导致报错失败
      .mode(SaveMode.Append)
      .format("jdbc")
      // 设置MySQL数据库相关属性
      .option("driver", ApplicationConfig.MYSQL_JDBC_DRIVER)
      .option("url", ApplicationConfig.MYSQL_JDBC_URL)
      .option("user", ApplicationConfig.MYSQL_JDBC_USERNAME)
      .option("password", ApplicationConfig.MYSQL_JDBC_PASSWORD)
      .option("dbtable", "itcast_ads_report.region_stat_analysis")
      .save()
    /*
    保存方式选择Append追加或覆写Overwrite，都会出现问题，所以在实际项目开发中，使用
SparkSQL分析数据报表报错数据库时，往往不会使用dataframe.write.jdbc方式。
     */
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
          | itcast_ads_report.region_stat_analysis
          |  (report_date,province,city,count)
          | VALUES(?,?,?,?)
          |  ON DUPLICATE KEY UPDATE
          |     count=VALUES(count)
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
        pstmt.setLong(4, row.getAs[Long]("count"))
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
//    saveResultToMySQL(resultDF)
    //将DataFrame转换为RDD操作,或者转换为Dataset操作
     resultDF.coalesce(1).rdd.foreachPartition(iter =>saveToMySQL(iter))


  }

}
