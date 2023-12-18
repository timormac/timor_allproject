package a2_classAndObject

/**
 * @Title: A4_AbstractClass
 * @Package: a2_classAndObject
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 12:26
 * @Version:1.0
 */
abstract class A4_AbstractClass {
  //抽象属性
  var age: Int
  //非抽象属性
  val name:String = "bob"

  //这里出问题了
//  var name:String = _

  //抽象方法:没有等号也没有大括号，返回值必须手动写
  def sleep(): Unit

  //非抽象方法
  def getProperty () = println(name + age)

}

//这里有个问题就是重写非抽象属性,用var 声明的name 下面改写不了
class InheritClass extends  A4_AbstractClass{

  //重写非抽象属性必须写override 并且 var 和: 也必须写
  override val name : String = "a"

  //这里有问题，注意这里声明使用var
  //var name:String = "a"

  //赋值抽象属性 var 和: 必须写
   var age: Int = _

  //实现抽象方法
  def sleep(): Unit = println("sleep")

  //重写方法
  override def getProperty(): Unit = super.getProperty()
}
