package com.atguigu.scala.chapter7

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/28 11:37
  */
object Test12_HighLevelFunctions_Reduce {
  def main(args: Array[String]): Unit = {

    val list = List(1,2,3,4)

    // 1. reduce
    val res1 = list.reduce( (a: Int, b: Int) => a + b )
    val res2 = list.reduce( _ + _ )
    val res3 = list.reduceLeft( _ + _ )
    val res4 = list.reduceRight( _ + _ )

    list.sum

    println(res1)
    println(res2)
    println(res3)
    println(res4)

    println("=============================")

    // 关于左右规约，减法规约
    println(list.reduceLeft( _ - _ ))    // -8
    println(list.reduceRight( _ - _ ))    // -2

    val list2 = List(3,4,5,8,10)
    println(list2.reduceLeft( _ - _ ))    // -24  3-4-5-8-10
    println(list2.reduceRight( _ - _ ))    // 6  3-(4-(5-(8-10)))

    // 2. fold
    val res5 = list.fold(10)(_ - _)
    //这个是用10减去数组中的数
    println(res5)

    println(list2.foldRight(11)(_ - _))   // -5  3-(4-(5-(8-(10-11)))

  }
}
