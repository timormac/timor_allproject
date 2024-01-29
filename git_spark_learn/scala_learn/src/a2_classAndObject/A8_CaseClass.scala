package a2_classAndObject

/**
 * @Author Timor
 * @Date 2024/1/23 9:19
 * @Version 1.0
 */

/**
 * 样例类特性
 * 属性默认为val，可以省略
 * 自动创建伴生对象,实现里面的apply方法，可以用伴生对象new对象
 *
 */
case class A8_CaseClass( name:String)

class NoCase(val  name:String )

object ttt{

  def main(args: Array[String]): Unit = {

    val tom = new NoCase("tom")
    println( tom.toString )

    //通过伴生对象的apply方法，创建对象
    val jetty =  A8_CaseClass("jetty")

    println( jetty.toString)



  }

}
