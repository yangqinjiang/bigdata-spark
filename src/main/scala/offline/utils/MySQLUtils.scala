package offline.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import offline.config.ApplicationConfig
import org.apache.spark.sql.Row

trait MySQLUtils {
  /**
   * 保存数据至MySQL数据库，使用函数foreachPartition对每个分区数据操作，主键存在时更新，不存在时插入
   *
   * @param iter
   */
  def saveToMySQL(iter: Iterator[Row],
                  db_table: String,
                  primaryKeyTupleSeq: Seq[(String, String)],
                  updateKeyTupleSeq: Seq[(String, String)]): Unit = {
    //加载驱动类
    Class.forName(ApplicationConfig.MYSQL_JDBC_DRIVER)
    //声明变量
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try {
      //b获取连接
      conn = DriverManager.getConnection(
        ApplicationConfig.MYSQL_JDBC_URL, //
        ApplicationConfig.MYSQL_JDBC_USERNAME, //
        ApplicationConfig.MYSQL_JDBC_PASSWORD
      )

      //问号占位符
      val h = "?" * (primaryKeyTupleSeq.size + updateKeyTupleSeq.size)
      val holder = h.split("").mkString(",").toString


      val primaryKeyTupleWithIndex: Seq[((String, String), Int)] = primaryKeyTupleSeq.zipWithIndex
      val primaryKey = {
        primaryKeyTupleSeq.map(tuple => tuple._1).mkString(",")
      }

      val updateKeyTupleWithIndex: Seq[((String, String), Int)] = updateKeyTupleSeq.zipWithIndex
      val updateKey: String = {
        updateKeyTupleSeq.map(_._1).mkString(",")
      }

      val duplicateKeyUpdate: Seq[String] = updateKeyTupleSeq.map(word => word._1 + "=VALUES(" + word._1 + ")")
      val duplicateKeyUpdateString = duplicateKeyUpdate.mkString(",")
      //c 获取preparedStatement对象
      val insertSql =
        s"""
           |INSERT INTO
           | ${db_table}
           |  (${primaryKey},${updateKey})
           | VALUES(${holder})
           |  ON DUPLICATE KEY UPDATE ${duplicateKeyUpdateString}
           |""".stripMargin

      pstmt = conn.prepareStatement(insertSql)
      val beforeCommitStatus = conn.getAutoCommit //保存一下原来的自动提交状态
      // 不自动提交
      conn.setAutoCommit(false)
      // 将分区中数据插入到表中,批量插入
      // 采用批量插入的方式将RDD分区数据插入到MySQL表中，提升性能
      iter.foreach { row =>

        //primaryKey
        primaryKeyTupleWithIndex.foreach(p => {
          val x: ((String, String), Int) = p

          x._1._2.trim.toLowerCase match {
            case "string" => pstmt.setString(x._2 + 1, row.getAs[String](x._1._1))
            case "double" => pstmt.setDouble(x._2 + 1, row.getAs[Double](x._1._1))
          }
        })
        //duplicateKeyUpdate
        updateKeyTupleWithIndex.foreach(p => {
          val x: ((String, String), Int) = p
          x._1._2.trim.toLowerCase match {
            case "long" => pstmt.setLong(x._2 + 1 + primaryKeyTupleWithIndex.size, row.getAs[Long](x._1._1))
            case "double" => pstmt.setDouble(x._2 + 1 + primaryKeyTupleWithIndex.size, row.getAs[Double](x._1._1))
          }

        })
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
}
