package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Transform_K_V_MapValues {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("b", 1), ("d", 1)), 2)

    //4.对Value扩大两倍
    kvRDD.mapValues(_ * 2)
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}
