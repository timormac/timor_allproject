package com.atguigu.scala.chapter7

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/26 15:57
  */
object Test05_ListBuffer {
  def main(args: Array[String]): Unit = {
    // 1. 创建可变List
    val list1: ListBuffer[Int] = new ListBuffer[Int]()
    val list2 = ListBuffer(1, 2, 3)

    // 2. 添加数据
    list1.append(12)
    list2.append(23, 57)
    list1.prepend(13, 22)

    list1 += 65 += 39
    11 +=: list2
    println(list1)
    println(list2)

    // 3. 合并List
    val list3 = list1 ++ list2
    list1 ++= list2
    list1 ++=: list2
    println(list1)
    println(list2)
    println(list3)

    // 4. 修改元素
    list3(2) = 33
    list3.update(4, 55)

    // 5. 删除元素
    list3.remove(6, 2)

    val list4 = list3 - 3

    list3 -= 23

    println(list3)
  }
}
