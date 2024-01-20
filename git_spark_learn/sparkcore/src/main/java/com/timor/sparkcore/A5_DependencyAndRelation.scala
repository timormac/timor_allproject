package com.timor.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Title: A5_DependencyAndRelation
 * @Package: com.timor.sparkcore
 * @Description:
 * @Author: lpc
 * @Date: 2024/1/9 20:02
 * @Version:1.0
 */
object A5_DependencyAndRelation {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("a1").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = context.makeRDD(List("a", "b", "c"))
    val map: RDD[String] = rdd.map( _+1 )
    println("map的血缘" + map.toDebugString)
    println("map的依赖" + map.dependencies)

    val filter: RDD[String] = map.filter(_ == "b")
    println("filter的血缘" + filter.toDebugString)
    println("filter的依赖" + map.dependencies)


    filter.collect().foreach(println)


  }

}
