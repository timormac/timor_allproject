package com.atguigu.scala.chapter7

import scala.collection.immutable.Queue
import scala.collection.mutable

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/28 15:29
  */
object Test14_Queue {
  def main(args: Array[String]): Unit = {
    val que = new mutable.Queue[String]()
    val que1 = mutable.Queue[String]("abc", "hello")

    // 进队
    que.enqueue("a", "c")
    que1.enqueue("bvd", "hi")
    que1.enqueue("e")

    println(que1)

    // 出队
    println(que.dequeue())
    println(que1.dequeue())
    println(que1.dequeue())
    println(que1.dequeue())
    println(que1)
    println(que)

    // 并行处理
//    (1 to 100).map( i => println(Thread.currentThread().getName) )
    (1 to 100).par.map( i => println(Thread.currentThread().getName) )
  }
}
