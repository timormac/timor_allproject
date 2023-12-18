package com.atguigu.scala.chapter7

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/26 15:31
  */

// 不可变list

object Test04_List {
  def main(args: Array[String]): Unit = {
    // 1. 创建List
    val list1: List[Int] = List(1,2,3,4)
    println(list1)

    println("=======================")

    // 2. 访问List元素，遍历
    println(list1(3))
    for( elem <- list1 ) println(elem)

    // 3. 向列表里添加数据
    val list2 = 10 +: list1
    val list3 = list2 :+ 24
    println(list1)
    println(list2)
    println(list3)

    println("===============================")
    // 4. 特殊的List，以及用它来构建列表
    val list4 = list1.::(52)
    val list5 = Nil.::(12)
    println(list4)
    println(list5)

    val list6 = 13 :: list5
    val list7 = 15 :: 24 :: 67 :: Nil
    println(list7)

    println("======================")

    // 5. 扁平化
    val list8: List[Any] = list6 :: list7
    println(list8)

    val list9: List[Int] = list6 ::: list7
    println(list9)

    val list10 = list6 ++ list7

  }
}