package a2_classAndObject


/**
 * @Title: A3_ExtendClass
 * @Package: a2_classAndObject
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 11:29
 * @Version:1.0
 */
object A3_ExtendClass {
  def main(args: Array[String]): Unit = {
    val a = new UserA("tom",18)
    println(a.name)
    println(a.age)
  }
}

class User(var name:String ,var age:Int)

//继承有参类,继承的类也要声明参数列表
class UserA(name:String,age:Int) extends User(name, age)
