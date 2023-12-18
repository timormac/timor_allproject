package a2_classAndObject

import scala.beans.BeanProperty

/**
 * @Title: A1_ClassDemo
 * @Package: a2_classAndObject
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/4 11:33
 * @Version:1.0
 */

//注意伴生对象必须写在伴生类一起,名字相同,而且object是消协
object A1_Demo{

  def main(args: Array[String]): Unit = {
    //这里有个bug，idea不识别类无法.new
    val demo = new A1_Demo("tom",18,"male",3.14F)
    val demo2 = new A1_Demo("tom",18,"male")
    println(demo.toString)
    println(demo2.toString)
    demo.printProperty
  }
}


//class小写，并且没有public
class A1_Demo() {

  //和java相同,属性必须赋值。不过java的写法为: String str; 默认会给str一个nul。
  //scala想获取默认值要这样写str:String = _
  var sex:String = _
  var name:String = "aa"
  //给属性一个默认值 int 默认值是0 String是null
  var age :Int = _
  //加注解后默认生成getter和setter
 @BeanProperty
  var money:Float=_

  //标记主构造器被调用
  println("主构造器")

  //辅助构造器,必须用到主构造器,或者其他辅构着器。
  //注意主构造器的参数列表由，定义类时括号内决定
  def this(name: String, age: Int, sex: String) {
    this()
    this.name = name
    this.age = age
    this.sex = sex
    println("调用辅1")

  }
  //可多个辅助构造器
  def this(name:String,age:Int,sex:String,money:Float) {
    this(name,age,sex)
    this.name=name
    this.age = age
    this.sex = sex
    this.money = money
    println("调用辅2")
  }

  //定义方法时完完整写法
  override def toString(): String = {
     s"name:${name} age:${age} money:${money}  " + super.toString
  }

  //省略大法，参数括号可以省略，返回值可以省,一行时大括号可以省略。 若有大括号时等号可以省略
  def printProperty  { println(s"name:${name} age:${age} money:${money} ")  }


}




