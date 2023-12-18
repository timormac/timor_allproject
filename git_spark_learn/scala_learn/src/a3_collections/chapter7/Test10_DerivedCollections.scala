package com.atguigu.scala.chapter7

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/28 10:10
  */
object Test10_DerivedCollections {
  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2, 3, 4, 5, 6, 7)
    val list2 = List(5, 4, 6, 7, 8, 9, 10, 11, 12)

    //    （1）获取集合的头
    println(list1.head)

    //    （2）获取集合的尾（不是头的就是尾）
    println(list1.tail)

    //    （3）集合最后一个数据
    println(list2.last)

    //    （4）集合初始数据（不包含最后一个）
    println(list2.init)

    //    （5）反转
    println(list1.reverse)

    //    （6）取前（后）n个元素
    println(list1.take(3))
    println(list2.takeRight(5))

    //    （7）去掉前（后）n个元素
    println(list1.drop(3))

    println("=====================")

    //    （8）并集
    val union = list1.union(list2)
    println(union)
    println(list2.union(list1))

    val set1 = Set(1, 2, 3, 4, 5, 6, 7)
    val set2 = Set(4, 5, 6, 7, 8, 9, 10, 11, 12)
    val unionSet = set1.union(set2)
    println(unionSet)
    println(set2.union(set1))

    //    （9）交集
    val intersectionSet = set1.intersect(set2)
    println(intersectionSet)

    println(list1.intersect(list2))
    println(list2.intersect(list1))

    //    （10）差集
    println(list1.diff(list2))
    println(list2.diff(list1))

    //    （11）拉链
    println(list1.zip(list2))

    println("=============================")

    //    （12）滑窗
    // 12.1 单参数，步长为1
    for( elem <- list1.sliding(5)) println(elem)

    // 12.2 两参数，size和步长
    for( elem <-  list2.sliding(5, 3)) println(elem)

    // 12.3 整体滑动
    for( elem <-  list2.sliding(3, 3)) println(elem)


    // 13. 初级计算函数
    println(list1.sum)
    println(list1.product)

    val list3: List[(String, Int)] = List(("a", 1), ("b", 2), ("c", 3))
    println(list2.max)
    println(list3.maxBy( data => data._2 ))

    // 排序
    println(list2.sorted(Ordering[Int].reverse))
    println(list3.sortBy( _._2 )(Ordering[Int].reverse))
    println(list3.sortWith( (data1, data2) => data1._2 < data2._2 ))
    println(list3.sortWith( _._2 > _._2 ))
  }
}
