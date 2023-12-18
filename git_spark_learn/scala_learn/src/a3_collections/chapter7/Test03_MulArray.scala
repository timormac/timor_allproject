package com.atguigu.scala.chapter7

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/26 15:15
  */
object Test03_MulArray {
  def main(args: Array[String]): Unit = {
    // 1. 创建二维数组
    val arr: Array[Array[Int]] = Array.ofDim[Int](2, 3)

    // 2. 访问数组元素
    println(arr(0)(1))
    arr(0)(1) = 13
    println(arr(0)(1))

    // 3. 遍历数组
    for( i <- 0 until arr.length; j <- 0 until arr(i).length ) println(arr(i)(j))
    for( i <- arr.indices; j <- arr(i).indices ){
      print(arr(i)(j) + "\t")
      if( j == arr(i).length - 1 ) println()
    }

    arr.foreach( line => line.foreach(println) )
    arr.foreach( _.foreach(println) )
  }
}
