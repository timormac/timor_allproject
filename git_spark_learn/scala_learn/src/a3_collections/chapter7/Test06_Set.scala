package com.atguigu.scala.chapter7

import scala.collection.mutable

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/26 16:23
  */
object Test06_Set {
  def main(args: Array[String]): Unit = {
    // 1. 不可变Set
    // 1.1 创建
    val set1: Set[Int] = Set(1,4,2,5,5,27,67)
    println(set1)

    println("===================")

    // 1.2 添加元素
    val set2 = set1 + 21
    println(set1)
    println(set2)

    println("===================")

    // 1.3 合并set
    val set3 = Set(1, 27, 43, 79)
    val set4 = set2 ++ set3
    println(set4)

    // 1.4 删除元素
    val set5 = set4 - 21
    println(set5)

    println("========================")

    // 2. 可变Set
    // 2.1 创建
    val set6: mutable.Set[Int] = mutable.Set(1,2,3,4)
    println("====================")

    // 2.2 添加元素
    set6.add(35)
    println(set6.add(1))
    println(set6)

    println("====================")

    // 2.3 删除元素
    println(set6.remove(2))
    println(set6)

    set6 -= 2
    val set7 = set6 - 2

    // 2.4 合并集合
    val set8 = set6 ++ set1
    println(set6)
    println(set8)

    set6 ++= set1
    println(set6)
  }
}
