package com.atguigu.scala.chapter7

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/28 14:12
  */
object Test13_WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 简单版本
    val stringList = List(
      "hello",
      "hello world",
      "hello scala",
      "hello spark in scala",
      "hello flink in scala"
    )

    // 1.1 按空格切分字符串，得到打散的word
    val wordList0 = stringList.map( str => str.split(" ") )
//    println(wordList0.flatten)

    val wordList = stringList.flatMap( _.split(" ") )
    println(wordList)

    // 1.2 按照单词自身作为key分组
    val groupMap: Map[String, List[String]] = wordList.groupBy( word => word )
    println(groupMap)

    // 1.3 对分组的word列表进行length统计，得到（word，count）
    val countMap: Map[String, Int] = groupMap.map( kv => (kv._1, kv._2.length) )

    println(countMap)

    // 1.4 排序取top3
    val sortList: List[(String, Int)] = countMap
      .toList    // 转换成list
//        .sortWith( (data1, data2) => data1._2 > data2._2 )
      .sortWith( _._2 > _._2 )    // 按照count值从大到小排序
      .take(3)    // 选取前3

    println(sortList)

    println("====================")

    // 2. 复杂版本
    val tupleList: List[(String, Int)] = List(
      ("hello world", 1),
      ("hello scala", 2),
      ("hello spark in scala", 3),
      ("hello flink in scala", 4)
    )

    // 将元组进行转换，变成一个大的String
    val newStringList: List[String] = tupleList.map( kv => (kv._1.trim + " ") * kv._2 )
    println(newStringList)

    // 以下完全相同
    val wordCountList = newStringList
      .flatMap( _.split(" ") )    // 切分并扁平化，打散所有word
      .groupBy( word => word )    // 基于word做分组
      .map( kv => (kv._1, kv._2.length) )
      .toList
      .sortBy( _._2 )(Ordering[Int].reverse)
      .take(3)

    val wordCountMap1 = tupleList.flatMap( kv => {
      val strList = kv._1.split(" ")
      strList.map( word => (word, kv._2) )
    })
      .groupBy( _._1 )

    val resultList =  wordCountMap1
      .toList
//      .map( kv => {
//        kv._2.reduce( (kv1, kv2) => (kv1._1, kv1._2 + kv2._2) )
//      } )
      .map( kv => {
        (kv._1, kv._2.map(_._2))
      } )
      .map( kv => (kv._1, kv._2.sum) )
      .sortBy(_._2).reverse
      .take(3)
    println(resultList)
  }
}
