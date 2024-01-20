package com.timor.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * @Title: A6_Cache
 * @Package: com.timor.sparkcore
 * @Description:
 * @Author: lpc
 * @Date: 2024/1/9 20:20
 * @Version:1.0
 */
object A6_Cache {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("a1").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = context.makeRDD(List("a", "b"))

    /**
     * 当map的rdd多次调用行动算子,如果不对map进行cache缓存，那么map会重新执行多次获取rdd
     * 缓存不会自动加入，需要手动添加
     */
    val map: RDD[String] = rdd.map(x => {
      println(x)
      x + "|1"
    })
    println("map:" + map.toDebugString)

    /**
     * 设置缓存级别，是否落硬盘等
     * 注意persist和cache只能二选一，cache是默认的内存，2个代码都写会报错，因为重复代码
     * 如果想内存就cache(),想设置别的等级，就直接persist()
     */
    //map.persist(StorageLevel.MEMORY_AND_DISK_2)

    //手动添加缓存,注意cache是在行动算子调用时才触发
    map.cache()

    //增加cache后，不会改变原来的血缘，会发现多了个血缘关系，以后获取rdd会优先从cache获取
    println("cache后:" + map.toDebugString)

    map.collect().foreach(println)
    map.collect().foreach(println)

  }

}
