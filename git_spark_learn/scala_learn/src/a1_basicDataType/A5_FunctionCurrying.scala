package a1_basicDataType

/**
 * @Title: A5_FunctionCurrying
 * @Package: a1_basicDataType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/4 23:40
 * @Version:1.0
 */
object A5_FunctionCurrying {

  def main(args: Array[String]): Unit = {

    //--------------------柯里化实现流程------------------------------

    def add_two(x: Int) = (y: Int) => x + y
    add_two(1)(2)

    //返回类型如果把括号省略就很像迭代式，不过不便于阅读 Int => Int => Int
    //实际上第一个参数的返回值是 （Int） => (Int => Int) 这种便于阅读
    def add_three(x: Int) = (y: Int) => (z: Int) => x + y + z
    val adda: Int => (Int => Int) = add_three(1)
    val addb: Int => Int = adda(2)
    val res: Int = addb(3)

    //--------------------第一种柯里化写法------------------------------
    //正确的柯里化理解。b是乘法是重要加权数字 f(a,b,c) = (a+c)*b
    def f_curr ( a:Int,b:Int,c:Int) :Int = (a+c)*b

    //获得一个函数,函数的参数用_代替
    val demo1_curr :Int=>Int =f_curr(1,_,3)
    println( "demo1_curr:" + demo1_curr(10))

    //获得一个函数,函数的参数用_代替,可替换多个
    val demo2_curr: (Int,Int) => Int = f_curr(_, _, 3)
    println("demo2_curr:" + demo2_curr(2,10))

    //--------------------第二种柯里化写法------------------------------

    //正确的柯里化理解。b是乘法是重要加权数字 f(a)(b)(c) = (a+c)*b
    def f_curr2( a:Int)(b:Int)(c:Int) :Int = (a+c)*b

    //获得一个函数,函数的参数用_代替,可替换多个
    val demo1_curr2 : Int =>Int = f_curr2(1)(_)(2)
    val i1 = demo1_curr2(10)
    println("demo1_curr2:"+ i1)

    //--------------------下面是错误理解的柯里化--------------------------

    //柯里化的作用f(x,y,z) 转化为 f(x)(y)(z)
    //f(1)(2)(3) = 1+2+3
    def fun_ab1(a: Int): Int => (Int => Int) = {
      def fun_ab2(b: Int): Int => Int = {
        def fun_ab3(c: Int): Int = a + b + c

        fun_ab3 _
      }

      fun_ab2 _
    }

    //???这个到底是个什么东西呢？b,c的值都还没传,怎么走的？？
    //问题已解决，看md文档的柯里化java实现
    val func_1: Int => (Int => Int) = fun_ab1(1)
    println(func_1)

    //
    val func_2: Int => Int = func_1(2)
    println(func_2)

    //
    val i = func_2(3)
    println(i)


  }

}
