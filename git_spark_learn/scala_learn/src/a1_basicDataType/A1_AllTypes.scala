package a1_basicDataType

/**
 * @Author Timor
 * @Date 2023/12/4 10:21
 * @Version 1.0
 */

//scala 为了表明完全面对对象，删除了static方法,类可以有直接调用的方法不合理,必须创建object对象才能调用,object是个单例对象
object A1_AllTypes {

  def main(args: Array[String]): Unit = {

    //var变量可变 ,val不可变。声明都用val,保证不会被更改指向
    //注意scla 不需要;号
    var age: Int = 6
    var id: Long = 0L
    var c:Char ='a'
    var double:Double = 6.2
    //必须有 f
    var float:Float=6.5f
    //必须有L
    var long:Long = 12334L
    var bool:Boolean = true
    val name: String = "a"

    //元组类型
    val tuple1: (String, Int, Double) = ("asd", 123, 6.2)

    //自动类型推断可不加类型
    val name1 = "a"
    println(s"s1:${name} name:${name1}")
    val sql =
      s"""
        |select *
        |from ${name}
        |""".stripMargin

  }

}



