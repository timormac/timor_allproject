package a3_collections.chapter7

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: scala0523
  * Package: com.atguigu.scala.chapter7
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/26 14:16
  */
object Test02_ArrayBuffer {
  def main(args: Array[String]): Unit = {
    // 1. 创建可变数组
    val arr1: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val arr2 = ArrayBuffer(1,2,3)
    println(arr1)
    println(arr2)

    println("=====================")

    // 2. 访问数组元素
    println(arr2(2))
    arr2(2) = 15
    arr2.update(1, 27)
    println(arr2)

    // 3. 向数组添加元素
    val newArr1 = arr1 :+ 23
    val newArr2 = 12 +: arr2
    println(arr1)
    println(arr2)
    println(newArr1)
    println(newArr2)

    println("======================")
    // 调用带英文名称的方法
    arr1.append(13)
    arr2.append(24, 78)
    arr1.prepend(36, 92)
    arr1.insert(1, 22, 18)
    println(arr1)
    println(arr2)

    arr1 += 31
    13 +=: arr1
    println(arr1)

    println("========================")

    // 4. 删除数组元素
    val n = arr1.remove(2)
//    arr1.remove(4, 2)
    println(n)
    println(arr1)

    arr1 -= 13
    println(arr1)

    // 还可以用++=进行集合合并
    arr2 ++= ArrayBuffer(23, 45, 67)
    Array(12, 56) ++=: arr2

    println(arr2)

    println("=====================")

    // 5. 可变数组和不可变数组的转换
    // 5.1 可变转不可变
    val arr: ArrayBuffer[Int] = ArrayBuffer(1,2,3)
    val newArr: Array[Int] = arr.toArray

    // 5.2 不可变转可变
    val buffer: mutable.Buffer[Int] = newArr.toBuffer
    println(buffer)

    val bArr = ArrayBuffer[Int]()
    bArr ++= newArr
    println(bArr)
  }
}
