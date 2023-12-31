package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Transform_K_V_CombineByKeyTest {

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
    kvRDD.combineByKey(
      (x: Int) => (x, 1),
      (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1),
      (a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2))
      .map { case (word, (sum, count)) =>
        (word, sum / count.toDouble)
      }
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}
