package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * shuffle:指的是父RDD的某个分区中的数据被子RDD多个分区继承
  */
object Spark10_Transform_V_Coalesce {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8), 4)

    //4.打印当前RDD的分区数
    println(s"valueRDD:${valueRDD1.getNumPartitions}")

    //5.缩减分区
    val valueRDD2: RDD[Int] = valueRDD1.coalesce(2, true)
    println(valueRDD2.getNumPartitions)

    //6.两个RDD的分区数据
    valueRDD1.mapPartitionsWithIndex((index, iter) => {
      iter.map(x => (index, x))
    }).collect().foreach(println)

    valueRDD2.mapPartitionsWithIndex((index, iter) => {
      iter.map(x => (index, x))
    }).collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}