import org.apache.spark.internal.Logging
import rt.config.ApplicationConfig
import rt.store.hbase.HBaseDao

object Test extends Logging{

  def main(args: Array[String]): Unit = {
    val strings: Array[String] =
      """
        |{"orderId":"20210725105938469000001","userId":"400000611","orderTime":"2021-07-25 10:59:38.469","ip":"171.11.53.148","orderMoney":"336.28","orderStatus":0,"province":"河南省","city":"商丘市"}
        |{"orderId":"20210725105939373000002","userId":"200000807","orderTime":"2021-07-25 10:59:39.373","ip":"182.86.237.169","orderMoney":"433.88","orderStatus":0,"province":"江西省","city":"吉安市"}
        |{"orderId":"20210725105941132000001","userId":"100000619","orderTime":"2021-07-25 10:59:41.132","ip":"222.65.73.123","orderMoney":"25.53","orderStatus":0,"province":"上海","city":"上海市"}
        |{"orderId":"20210725105941132000002","userId":"200000716","orderTime":"2021-07-25 10:59:41.132","ip":"106.95.236.248","orderMoney":"209.66","orderStatus":0,"province":"重庆","city":"重庆市"}
        |{"orderId":"20210725105944987000001","userId":"400000270","orderTime":"2021-07-25 10:59:44.987","ip":"171.10.93.121","orderMoney":"82.55","orderStatus":0,"province":"河南省","city":"郑州市"}
    """.split("|")
//    strings.foreach(println)

    var a = Seq("""{"orderId":"20210725105938469000001","userId":"400000611","orderTime":"2021-07-25 10:59:38.469","ip":"171.11.53.148","orderMoney":"336.28","orderStatus":0,"province":"河南省","city":"商丘市"}""")
    a.foreach(println)
    val isInsertSuccess: Boolean = HBaseDao.insert(
      ApplicationConfig.HBASE_ORDER_TABLE,
      ApplicationConfig.HBASE_ORDER_TABLE_FAMILY,
      ApplicationConfig.HBASE_ORDER_TABLE_COLUMNS,
      a.iterator
    )
    logWarning(s"Insert Datas To HBase: $isInsertSuccess")
  }
}
