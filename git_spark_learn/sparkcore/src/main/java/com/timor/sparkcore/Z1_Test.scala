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

    val conf = new SparkConf()
      .setAppName("a1")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册自定义类,不需要自己手动将类实现Kyyo序列化,不注册Perso，会报错
      .registerKryoClasses(Array(classOf[Searcher], classOf[Person1]))

    val context: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = context.makeRDD(List("a", "b","c"))
    val map: RDD[Person1] = rdd.map( new Person1(_) )
    val index: RDD[(Int, Person1)] = map.mapPartitionsWithIndex((index, iter) => {
      println(index)
      iter.map((index, _))
    })
    index.collect().foreach(println)





  }

}
class Person1(val name:String,val age:Int =18){

  override def toString = s"Person1($name, $age)"
}
