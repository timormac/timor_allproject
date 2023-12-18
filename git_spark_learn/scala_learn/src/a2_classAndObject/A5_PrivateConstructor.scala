package a2_classAndObject

/**
 * @Title: A5_PrivateConstructor
 * @Package: a2_classAndObject
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 12:45
 * @Version:1.0
 */
object A5_PrivateConstructor {

  def main(args: Array[String]): Unit = {

    //无法调用私有构造器
    //val clazz = new PrivateClass("tom")

    //通过伴生对象，不需要写new 默认调apply方法
    val tom = PrivateClass("tom")

  }

}

//构造器私有化
class PrivateClass private(var name:String ){
  //可以调用伴生对象的属性
  var age:Int = _
  var sex:String = PrivateClass.sex
  var money:Double = _

  //辅构造器私有化
  private  def this(name:String, age:Int) {
    this(name)
    this.age = age
  }

}

//伴生对象
object  PrivateClass{
  var sex :String = "male"

  //伴生对象能用私有构造器
  //在java中私有构造器一般都是类通过static方法去创建一个对象。
  // 因为scala一切都是对象，没有static方法,只能通过伴生对象去创建
  //名字必须是apply
  def apply(name:String):PrivateClass =  {  new PrivateClass(name) }

  //apply方法重载
  def apply (name:String ,age:Int,Sex:String) = {
    val obj = new PrivateClass(name, age)
    obj.sex = this.sex
    obj
  }
}