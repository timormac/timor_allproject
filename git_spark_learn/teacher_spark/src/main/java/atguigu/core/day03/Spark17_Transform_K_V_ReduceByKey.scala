package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark17_Transform_K_V_ReduceByKey {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 1), ("a", 1), ("d", 1)), 2)
    val kvRDD2: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (1, "c"), (3, "d")), 2)

    //4.统计每个字母出现的次数
    println(kvRDD1.reduceByKey(_ + _).partitioner.get)
    kvRDD2.reduceByKey(_ + _).collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}