package sql

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * SparkSQL 启动ThriftServer服务，通过JDBC方式访问数据分析查询
 * i). 通过Java JDBC的方式，来访问Thrift JDBC/ODBC server，调用Spark SQL，并直接查询Hive中的数据
 * ii). 通过Java JDBC的方式，必须通过HTTP传输协议发送thrift RPC消息，Thrift JDBC/ODBC server必须通过上面命
 * 令启动HTTP模式
 */
object SparkThriftJDBC {

  def main(args: Array[String]): Unit = {
    //定义相关实例对象,未进行初始化
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null

    try {
      //加载驱动类
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      //获取连接connection
      conn = DriverManager.getConnection(
        "jdbc:hive2://hadoop102:10000/default",
        "root",
        "123456"
      )
      // 构建查询语句
      val sqlStr: String =
        """
          |select * from score
          |""".stripMargin

      pstmt = conn.prepareStatement(sqlStr)
      //执行查询,获取结果
      rs = pstmt.executeQuery()
      //打印查询结果
      while (rs.next()) {
        println(s"name = ${rs.getString(1)} , subject = ${rs.getString(2)} , score = ${rs.getInt(3)}")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != rs) rs.close()
      if (null != pstmt) pstmt.close()
      if (null != conn) conn.close()
    }
  }
}
