package a1_basicDataType

/**
 * @Title: A3_FunctionReturns
 * @Package: a1_basicDataType
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/4 19:01
 * @Version:1.0
 */
object A3_FunctionDefine{

  def main(args: Array[String]): Unit = {

    //函数和方法的区别：在类中的是方法，方法可以重载。在方法或者函数中定义的叫函数,函数不能重载会报错
    //定义函数完整，返回值取最后一行，不用写return
    def func1(a: Int): String = {
      println(a)
      a + ""
    }

    //可变参数
    def func(s:Int*)= for (elem <- s) println(elem)
    func(1,2,3)

    //参数默认值
    def fucc(name:String="无名",age:Int=18,money:Double=3.14   )= println(name+age+money)

    //默认按顺序传递,或者手动按参数名字传递
    fucc("tom")
    fucc(age=89)

    //参数可以是一个函数:意义是要求的参数函数的格式为：2个int类型参数,返回值是int
    def funcArgs(f: (Int, Int) => Int):Int = f(1, 3) * 10


    //手动return函数的返回值必须手动指定,不然报错
    def demo(a: Int): Any = {
      if (a == 1) return "1"
      print(1)
      if (a > 10) return "a>10"
      else return 1
    }

    //--------------------函数省略大法--------------------------

    //省略大法:返回值省略,当无参时参数()省略，并且不复杂时函数{}也可以省略
    def func2 = println(1)

    //当函数期望是无返回值类型=号也能省略,不过大括号不能省略
    def func7 { println(1) }

    //当调用函数无参时()也可以省略,就是相当于函数等于函数了
    def func3 = println

    //--------------------逻辑语句返回值-----------------------

    //if-else条件语句是有返回值的,具体看所有分支，返回结果是所有分支父类
    def ifReturn(a: Int) = if (a > 1) 1  else true

    //注意没有else收尾分支,可能出现匹配不到情况，所以返回值是Unit和分支结果的父类
    def noElseReturn(a:Int) = if(a>1) ""

    //for循环返回值是Unit
    def forReturn(a: Int) =   for(i <- 1 to a) i

    //用yield返回的是将3*i 放入到一个vetor中返回，不过不知道为什么类型推断不显示
    def yieldReturn  = for(i<- 1 to 6) yield i*3
    println(yieldReturn)

  }

}
