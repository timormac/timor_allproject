package com.atguigu.scala.chapter7

import scala.collection.mutable

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/26 16:40
  */
object Test07_Map {
  def main(args: Array[String]): Unit = {
    // 1. 不可变Map
    // 1.1 创建
    val map1: Map[String, Int] = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
    println(map1)

    // 1.2 遍历map元素
    map1.foreach(println)
    map1.keys.foreach(println)
    map1.values.foreach(println)

    for( elem <- map1 ) println(elem)

    val iter = map1.iterator
    while(iter.hasNext)
      println(iter.next())

    println("========================")

    // 1.3 单独访问元素
    println(map1.get("c").getOrElse(0))
    println(map1.getOrElse("c", 0))

    println("================================")

    // 2. 可变Map
    // 2.1 创建
    val map2: mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)

    // 2.2 添加元素
    map2.put("d", 67)
    map2.put("e", 13)

    map2 += ("f" -> 25)
    println(map2)

    // 2.3 修改和删除
    map2.update("d", 31)
    println(map2("b"))
    map2("b") = 69

    map2.remove("c")
    map2 -= "c"
    println(map2)

    // 2.4 合并两个Map
    val map3 = map1 ++ map2
    map2 ++= map1
    println(map1)
    println(map2)
    println(map3)
  }
}
