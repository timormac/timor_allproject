package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_Transform_K_V_GroupByKey {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 1), ("a", 1), ("d", 1)), 2)

    //4.按照key进行分组
    val value: RDD[(String, Iterable[Int])] = kvRDD1.groupByKey()

    //5.统计每个字母出现的次数
    value.map { case (value, iter) =>
      (value, iter.size)
    }.collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}