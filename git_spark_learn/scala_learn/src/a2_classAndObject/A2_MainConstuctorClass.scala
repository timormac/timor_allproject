package a2_classAndObject

/**
 * @Title: A2_MainConstuctorClass
 * @Package: a2_classAndObject
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 11:04
 * @Version:1.0
 */


object A2_MainConstuctorClass {
  def main(args: Array[String]): Unit = {
    val s1 = new Student2("tom", 18, "male")
    val s2 = new Student2("tom", 18, "male",3.14F)
    s1.printProperty()
    s1.printProperty()
  }
}


//不推荐
class Student1{
  var name:String = _
  var age:Int = _
}

//语法糖,_表示默认值,等同于var name:String的缩写
class Studen3(_name:String,_age:Int)

//当类有3个属性必传时,这种写法，生成一个有三参数的主构造器,不设置则为默认属性
class Student2(var name:String,var age:Int=18 ,val sex:String){

  var money:Float = _
  //4参数辅助构造器
  def this(name:String, age:Int ,sex:String,money:Float){
    this(name,age,sex)
    this.money = money
  }

  def printProperty() : Unit =  println(name+age+sex+money)
}


