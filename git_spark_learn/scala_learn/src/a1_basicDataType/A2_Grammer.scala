package a1_basicDataType

/**
 * @Title: A2_Grammer
 * @Package: a1_basicDataType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/4 18:45
 * @Version:1.0
 */
object A2_Grammer {
  def main(args: Array[String]): Unit = {

    //  + - * / %
    //  > <  >=  <=  == !=
    // 注意scala中的==和java不同，java是比对地址值，而scala是比对值,scala的.eq才是比对地址
    val s1 = "abc"
    val s2 = new String("abc")
    println( s1 == s2)  //true
    //.eq方法才是比对地址
    println( s1.eq(s2)) //false



  }

}
