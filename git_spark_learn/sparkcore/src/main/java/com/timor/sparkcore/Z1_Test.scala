package com.timor.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Title: Z1_Test
 * @Package: com.timor.sparkcore
 * @Description:
 * @Author: lpc
 * @Date: 2024/1/8 15:43
 * @Version:1.0
 */
object Z1_Test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("a1").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = context.makeRDD(List("a", "b"))
    val map: RDD[String] = rdd.map( x =>{
      println(x)
      x+"|1"
    })
    map.cache()

    map.collect().foreach(println)
    map.collect().foreach(println)




  }

}
class Person1(val name:String,val age:Int =18){

  override def toString = s"Person1($name, $age)"
}
