package com.atguigu.scala.chapter7

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/28 9:19
  */
object Test08_Tuple {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个元组
    val tuple1: (Int, String, Double, Char) = (100, "hello", 12.3, 'c')
    println(tuple1)

    // 2. 访问元组数据
    println(tuple1._3)
//    tuple1._2 = ""  // error, 不能重新赋值

    println(tuple1.productElement(0))

    // 3. 遍历数据
//    for( elem <- tuple1 ) println(elem)    // 没有foreach方法
//    tuple1.foreach()
    for( elem <- tuple1.productIterator) println(elem)

    // 4. 元组和map的键值对
    val map1 = Map(("a", 1), ("b", 2), ("c", 3), ("d", 4))
    println(map1)

    // 5. 嵌套元组
    val tuple2: (Int, Double, String, List[Int], (Char, Int)) = (12, 0.9, "hi", List(1, 2), ('c', 15))
    println(tuple2._5._2)

    val tuple3 = (1,1,2,2)
    val tuple4 = (("a", 1), ("a", 2))
    println(tuple4)

  }
}
