package com.atguigu.scala.chapter7

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/28 11:10
  */
object Test11_HighLevelFunctions_Map {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5,6,7,8,9)

    // 1. 过滤
    // 筛选所有的偶数
    val newList = for( i <- list if i % 2 == 0 ) yield i
    println(newList)

    val evenList = list.filter( _ % 2 == 0 )
    println(evenList)

    // 2. map
    println(list.map(_ * 2 + " data"))

    // 3. 扁平化
    val nestedList: List[List[Int]] = List(List(1,2,4), List(3,7,9), List(12,23,45))
    val flatList = nestedList.flatten
    println(flatList)

    // 4. 扁平映射
    val stringList: List[String] = List("hello scala", "hello world", "hello atguigu")
    println(stringList.map(str => str.split(" ")).flatten)
    println(stringList.flatMap( _.split(" ") ))

    // 5. 分组
    // 按照奇偶性分组
    val groupedMap: Map[String, List[Int]] = list.groupBy( data => if(data % 2 == 0) "偶数" else "奇数" )
    println(groupedMap)

    // 按照字符串首字母分组
    val wordList: List[String] = List("china", "canada", "usa", "japan", "uk")
    println(wordList.groupBy( _.charAt(0) ))
  }
}
