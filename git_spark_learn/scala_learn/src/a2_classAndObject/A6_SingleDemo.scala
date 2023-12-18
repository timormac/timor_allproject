package a2_classAndObject

/**
 * @Title: A6_SingleDemo
 * @Package: a2_classAndObject
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 13:06
 * @Version:1.0
 */
object A6_SingleDemo {
  def main(args: Array[String]): Unit = {
    val tom = SingleCLass("tom")
    val jack = SingleCLass("jack")
    println(tom)
    println(jack)
  }
}

class SingleCLass private(name:String){ }

//单例模式
object SingleCLass{
  private var instance :SingleCLass = _

  def apply(name:String): SingleCLass = {
    if( instance == null )   instance = new SingleCLass(name)
    instance
  }
}
