package com.timor.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Title: A0_SparkContext
 * @Package: com.timor.sparkcore
 * @Description:
 * @Author: lpc
 * @Date: 2024/1/9 17:44
 * @Version:1.0
 */
object A0_SparkContext {

  private val conf: SparkConf = new SparkConf()
    .setAppName("app")
    .setMaster("local[*]")
    //替换成kyro序列化
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 注册自定义类,不需要自己手动将类实现Kyyo序列化,不注册Perso，会报错
    .registerKryoClasses(Array(classOf[Searcher], classOf[Person]))

  val sc: SparkContext = new SparkContext(conf)
  private val rdd: RDD[String] = sc.makeRDD(List("a,", "b", "c"))

  /**
   * word里的例子不好，没人会用这种方式去，定义一个类然后进行rdd操作
   * 一般都是rdd的算子内,用到了外部的变量List，或者算子内用到了自定义Dao的数据封装类型
   * 不过也有可能，你定义了一些工具包装类，那么这些包装类是要注册Kyro序列化或者实现Searilizer序列化的
   */

  //需要注册Person不然报错
  private val map: RDD[Person] = rdd.map(new Person(_))

  map.collect().foreach(println)


}

class Person(val name:String,val age:Int =18)

class Searcher(val query: String) {
  def isMatch(s: String) = {
    s.contains(query)
  }
  def getMatchedRDD2(rdd: RDD[String]) = {
    val q = query
    rdd.filter(_.contains(q))
  }
}
