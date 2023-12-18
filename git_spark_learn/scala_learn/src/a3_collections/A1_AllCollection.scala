package a3_collections

import scala.collection.mutable.ArrayBuffer

/**
 * @Title: A1_AllCollection
 * @Package: a3_collections
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 22:00
 * @Version:1.0
 */
object A1_AllCollection {
  def main(args: Array[String]): Unit = {

    //基本数组 不可变
    val arr1 = new Array[Int](5)
    //赋值
    arr1(0)=10
    //Array没找到apply方法
    println(arr1.apply(0))

    //源码没看到，懂为什么能动态传参数,只有一个apply构造器,参数是Array(_length)是什么意思
    //语法糖
    //另一种写法，用伴生对象
    val arr2 = Array(1, 2, 3, 4)

    //---------------------------可变数组-----------------------------------—

    //可变数组
    val arrBuff = new ArrayBuffer[Int](5)
    arrBuff(2)=20

    //数组中添加元素,返回新的
    val newArrbuff = arrBuff:+ 23

    //添加元素
    arrBuff.append(1,2,3)
    arrBuff.prepend(1,2,3)
    arrBuff += 31


  }




}
