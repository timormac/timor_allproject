package com.timor.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Title: A1_Operator
 * @Package: com.timor.sparkcore
 * @Description:
 * @Author: lpc
 * @Date: 20可配置参数23/12/7 21:41
 * @Version:1.0
 */
object A1_Operator {
  def main(args: Array[String]): Unit = {

    //注意scala要想类提示,要先new。 "local[*]"固定写法，本地模式
    val conf = new SparkConf().setAppName("a1").setMaster("local[*]")
    val context = new SparkContext(conf)
    //读取文件
    //val rdd:RDD[String] = context.textFile("/Users/timor/Desktop/data/input/wordcount/wordcount.txt")



    val rdd = context.makeRDD(List("a", "b", "c", "d"))



    //flatmap传入一个返回是可迭代的类型函数
    val flatmap:RDD[String] = rdd.flatMap(_.split(" ") )
    //map
    val map:RDD[(String,Int)]= flatmap.map( s=> ("start"+s,1 ) )

    //以分区为单位整体处理数据,Iterator=>Iterator；用法在这里创建连接
    //map.mapPartitions( _.map(_) )
    //mapPartitionsWithIndex

    //groupby,这里有个问题_不行，必须s=>s
    val groupBy:RDD[(String,Iterable[String] )] = flatmap.groupBy(s=>s)


    //reduceBykey
    val value:RDD[(String,Int)] = map.reduceByKey(_ + _)

    val tuples = value.collect()

    tuples.foreach(println)

    context.stop()

  }

}
