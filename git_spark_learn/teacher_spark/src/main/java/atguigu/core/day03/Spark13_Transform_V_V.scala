package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Transform_V_V {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val valueRDD2: RDD[Int] = sc.makeRDD(Array(3, 4, 5, 6), 2)

    //4.并集
    valueRDD1.union(valueRDD2).collect().foreach(println)

    //5.交集
    println("***********")
    valueRDD1.intersection(valueRDD2).collect().foreach(println)

    //6.差集
    println("***********")
    valueRDD1.subtract(valueRDD2).collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}