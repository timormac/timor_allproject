package a2_classAndObject

/**
 * @Title: A7_TraitDemo
 * @Package: a2_classAndObject
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 13:34
 * @Version:1.0
 */
object A7_TraitDemo {

  def main(args: Array[String]): Unit = {
    val demo1 = new DemoClass1()
    val demo2 = new DemoClass2()

    //trait动态混入,用途比如不该类的源码实现序列化方法
    val demo3 = new DemoClass2() with Trait3 {
      override def traitMethod2: Unit = println("aa")
      override def traitMethod3: Unit = println("bb")
    }

  }
}


abstract class TraitClass {
  val name ="tom"
  def classMethod1:Unit
}

trait Trait1{
  var property1:String
  def traitMethod1:Unit
}

trait Trait2{
  def traitMethod2:Unit
}

//trait可以继承trait
trait Trait3 extends  Trait1{
  def traitMethod3:Unit
}
trait Trait4{
  def traitMethod4:Unit
}


//Trait可以继承,多个Trait要用wih,每个trait单独with
class DemoClass1 extends Trait2 with Trait1{
  override var property1: String = _
  override def traitMethod1: Unit =  println("tM1")
  override def traitMethod2: Unit = println("tM2")

}
//继承抽象类,实现trait,每个trait单独with
class DemoClass2 extends TraitClass with Trait1 with Trait2 {
  override var property1: String = _
  override def traitMethod1: Unit =  println("tM1")
  override def classMethod1: Unit = println("cM1")
  override def traitMethod2: Unit = println("cM1")
}

class DemoClass3() extends  Trait3{
  override def traitMethod3: Unit = println("cM1")
  override var property1: String = _
  override def traitMethod1: Unit = println("cM1")
}