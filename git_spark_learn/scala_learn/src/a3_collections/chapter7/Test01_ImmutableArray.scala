package a3_collections.chapter7

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/26 11:20
  */

// 不可变数组

object Test01_ImmutableArray {
  def main(args: Array[String]): Unit = {
    // 1. 创建数组
    // 1.1 直接new
    val arr1: Array[Int] = new Array[Int](5)
    println(arr1.mkString(","))
    // 1.2 用伴生对象
    val arr2 = Array(1,2,3,4,5)
    println(arr2.mkString(","))

    println("===================")

    // 2. 访问数组中元素
    println(arr2(4))
    println(arr2.apply(2))

    arr1(1) = 23
    println(arr1(1))

    println("=====================")

    // 3. 遍历
    // 3.1 遍历下标
    for( i <- 0 until arr1.length ) println(arr1(i))
    for( i <- arr1.indices ) println(arr1(i))
    // 3.2 直接遍历数组
    for( elem <- arr1 ) println(elem)
    // 3.3 迭代器
    val iter = arr2.iterator
    while(iter.hasNext)
      println(iter.next())

    // 3.4 调用foreach方法
    arr1.foreach( (elem: Int) => println(elem) )
    arr2.foreach( println )

    println("=========================")
    // 4. 用不可变数组实现添加数据
    // 必须定义一个新的数组来接收返回值
    val newArr1 = arr1.:+(27)    // 在数组末尾加上一个数据
    val newArr2 = arr2.+:(13)
    println(newArr1.mkString(","))
    println(newArr2.mkString(","))

    val newArr3 = newArr1 :+ 35 :+ 13
    val newArr4 = 21 +: 19 +: newArr2 :+ 56
    println(newArr3.mkString(","))
    println(newArr4.mkString(","))
  }
}
