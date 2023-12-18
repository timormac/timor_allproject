package com.atguigu.scala.chapter7

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/28 9:39
  */
object Test09_CommonOp {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)

    //    （1）获取集合长度
    println(list.length)

    //    （2）获取集合大小
    println(list.size)

    val set = Set(1,2,3)
    set.size

    //    （3）循环遍历
    list.foreach(println)

    //    （4）迭代器
    for( elem <- list.iterator ) println(elem)
    val iter = list.iterator
    while(iter.hasNext) println(iter.next())

    //    （5）生成字符串
    val res = list.mkString("-")
    println(res)

    //    （6）是否包含
    println(list.contains(1))
    println(list.contains(7))
  }
}
