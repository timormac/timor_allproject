package com.timor.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Title: A7_Checkpoint
 * @Package: com.timor.sparkcore
 * @Description:
 * @Author: lpc
 * @Date: 2024/1/9 20:27
 * @Version:1.0
 */
object A7_Checkpoint {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("a1").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)

    //设置检查点路径
    context.setCheckpointDir("./ck1")

    val rdd: RDD[String] = context.makeRDD(List("a", "b"))

    val map: RDD[String] = rdd.map(x => {
      println(x)
      x + "|1"
    })
    println( "map:"+ map.toDebugString )

    //如果不cache,那么做checkpoint时,会重跑一次rdd生成流程，所以加cache
    map.cache()
    println( "cache后:"+map.toDebugString )


    /**
     *  checkpoint和cache区别： ck可以设置hdfs路径是高可用。,cache当设置存在磁盘时也是本地，不是高可用
     *  checkpoint会切断血缘关系，cache不会
     */
    //对map的rdd进行检查点
    map.checkpoint()
    println( "ck后:"+map.toDebugString )

    map.collect().foreach(println)

  }

}
