package a1_basicDataType

/**
 * @Title: A4_HigherFunction
 * @Package: a1_basicDataType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/4 21:21
 * @Version:1.0
 */
object A4_HigherFunction {

  def main(args: Array[String]): Unit = {

    //--------------------------lamda表达式用变量接------------------------

    //变量去接lamda。底层操作,创建一个类,实现lamda方法，然后创建对象,instance就是一个对象,类型是Int=>Int
    val instance = (x: Int) => 10 * x
    //注意下面的写法是错误的，lamda本身整体就是个匿名对象,不能写:
    //(x:Int) :Int => 10*x

    //通过对象调用方法
    instance(5)
    //Test$$$Lambda$1/931919113@7cf10a6f  instance就是一个对象所以打印的是地址
    println(  "printa" + instance)

    //---------------------------lamda表达式用函数接-------------------------

    //用def函数去接是是记录lamda本身,是没法直接用的，只能先获取对象再调用lamdaF1()返回值类型是Int=>Int
    def lamdaF1():Int=>Int = (x: Int) => 10 * x

    //先获取对象再调用
    lamdaF1()(5)

    //语法糖当用函数去接lamda时,无参省略括号时，调用必须不能带括号
    def lamdaF1Sugar = (x: Int) => 10 * x

    //这里调用函数不能带括号，不然会报错
    val instanceSugar:Int=>Int = lamdaF1Sugar

    //所以调用变量实例的时候只有一个括号，语法糖很恶心
    instanceSugar(10)


    //---------------------------函数的参数为函数-------------------------

    //参数可以是一个函数:意义是要求的参数函数的格式为：2个int类型参数,返回值是int
    //实际上真正的参数是传进去的是(实现lamda方法类)创建出来的对象
    def funcArgs(f: (Int) => Int): Int = f(3) * 10

    //会创建一个lamda实现对象，传给funcArgs
    funcArgs(  lamdaF1() )

    def func( x:String) = { println(x) }
    //已经定义的函数和lamda不通,如果想把函数本身创建的对象传给比变量用 _
    val url3  = func _

    //Test$$$Lambda$1/931919113@7cf10a6f
    println("printb" + url3)

    //语法糖: 手动指定变量对象,可以不用_去接,能自动识别
    val url4: String=>Unit = func

    //--------------------函数的返回值为函数--------------------------

    //装饰器功能
    def funcReturnFunc( f:Int=>Int ) = {
      //将f函数功能又做了操作
      def newFunc( i:Int, fc:Int=>Int ) = fc(i)*10
      newFunc _
    }

    //--------------------lamda省略大法--------------------------

    def getFunc(f: (Int,Int) => Int): String = f(1,2) + " "

    getFunc(  (x:Int,y:Int) => {
        println("---")
        x*2
      }
    )

    //参数类型能推断,可省。参数只一个，参数括号可省。逻辑体一行{}可省。
    getFunc( (x,y) => x*y )

    //若逻辑体只用到了一次参数，参数名_代替,并且 =>可以省略
    getFunc( _*_ )
  }

  //--------------------高阶函数练习--------------------------

  //f(0)("") = true
  def f_demo1(i: Int): String => Boolean = {
    def f_demo2(str: String): Boolean = {
      if (i == 0 && str == "") true
      else false
    }
    f_demo2 _
  }
  //true
  println("printc" + f_demo1(0)(""))

  //f(0)("") = true 简单的写法
  def f1_demo1(i: Int): String => Boolean = {
    str => if (i == 0 && str == "") true  else false
  }
  //true
  println( "printd" +  f1_demo1(0)(""))


}
