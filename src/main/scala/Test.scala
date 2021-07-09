import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term

object Test {

  def main(args: Array[String]): Unit = {
    import java.util
    val terms:  util.List[Term] = HanLP.segment("中华共和国")
    // 将java中集合对转换为scala中集合对象
    import  collection.JavaConverters._
    terms.asScala.map{term => (term.word,1)}.foreach(println)
//    import collection.mutable._
////    terms
//
//    val javaList: util.List[Int] = ArrayBuffer(1,2,3).asJava
//    println(javaList)
  }
}
