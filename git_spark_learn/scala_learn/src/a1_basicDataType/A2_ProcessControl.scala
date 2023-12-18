package a1_basicDataType

import scala.util.control.Breaks

/**
 * @Title: A2_ProcessControl
 * @Package: a1_basic
 * @Description:
 * @Author: XXX
 * @Date: 2023/9/18 14:10
 * @Version:1.0
 */
object A2_ProcessControl {

  def main(args: Array[String]): Unit = {

    //if分支
    if( 2>1 ) println("a")
    else if(3>2 )  println("b")
    else  println("c")

    //for循环 <- 起始数  to  终止，<- to是前开后开，用until 代替to是左闭右开,默认是加1
    for ( i <- 1 to 3){ println(i) }
    for( i <- 1 until 3){ println(i) }

    //控制步长每次+2
    for(i <-1 to 5 by 2){ println(i)}

    //增强for循环
    val arr=Array(1,2,3)
    for (elem <- arr)  println(elem)

    //while 循环
    var i:Int=0;

    while(i<5){
      println(i)
      i+=1
    }

    //do while 循环先执行一次
    var m:Int=0
    do{
      println(m)
      i+=1
    }while(i<10)

    //跳出循环,break  先导入scala.util.control.Breaks
    Breaks.breakable(
      for(i<-1 to 3){
        println(i)
        if(i==2) Breaks.break()
      }
    )

  }

}
