import org.apache.spark.internal.Logging

object Test extends Logging{

  def main(args: Array[String]): Unit = {
    val a = Seq("""{"orderId":"20210725105938469000001","userId":"400000611","orderTime":"2021-07-25 10:59:38.469","ip":"171.11.53.148","orderMoney":"336.28","orderStatus":0,"province":"河南省","city":"商丘市"}""")
    val datas: Iterator[String] = a.iterator
    println("size = " + datas.size)
    println("size = " + datas.size)
    datas.foreach(println)
    val nonEmpty: Boolean = testIterator(
      datas
    )
    logWarning(s"nonEmpty??: $nonEmpty")
  }

  def testIterator(datas:Iterator[String]):Boolean = {
    datas.nonEmpty
  }
}
