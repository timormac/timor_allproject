package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Transform_K_V_Test02 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 88), ("b", 95), ("a", 91),
      ("b", 93), ("a", 95), ("b", 98))
      , 2)

    //4.求每个key对应value的和以及出现的次数
    kvRDD.aggregateByKey((0, 0))(
      (x, y) => (x._1 + y, x._2 + 1),
      (a, b) => (a._1 + b._1, a._2 + b._2))
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}
