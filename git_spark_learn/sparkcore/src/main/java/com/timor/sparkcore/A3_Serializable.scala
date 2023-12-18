package com.timor.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Title: A3_Serializable
 * @Package: com.timor.sparkcore
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/8 00:02
 * @Version:1.0
 */
object A3_Serializable {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("SerDemo")
      .setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册自定义类
      .registerKryoClasses(Array(classOf[Searcher]))

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    val searcher = new Searcher("hello")
    val result: RDD[String] = searcher.getMatchedRDD1(rdd)

    result.collect.foreach(println)
  }

}

class Searcher(val query: String) {

  def isMatch(s: String) = {
    s.contains(query)
  }

  def getMatchedRDD1(rdd: RDD[String]) = {
    rdd.filter(isMatch)
  }

  def getMatchedRDD2(rdd: RDD[String]) = {
    val q = query
    rdd.filter(_.contains(q))
  }
}



