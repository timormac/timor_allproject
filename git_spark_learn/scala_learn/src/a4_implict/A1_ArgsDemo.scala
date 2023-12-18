package a4_implict

/**
 * @Title: A1_ArgsDemo
 * @Package: a4_implict
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 22:39
 * @Version:1.0
 */
object A1_ArgsDemo {


  def main(args: Array[String]): Unit = {

    implicit val intToDouble:  Double => Int = _.toInt

    //当没有隐身式f1函数时,编译会报错
    //scala会自己,没有把double变int的函数，然后找到f1了
    val num :Int = 3.5
    //结果是3
    println(num)

  }

  //隐式函数有切只有一个参数
 // implicit  def f1(d:Double):Int = d.toInt


}


