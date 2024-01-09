package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Transform_K_V_SortByKey {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", -3), ("e", -2), ("c", -4),
      ("b", -3), ("d", -6), ("c", -8)
    ), 2)

    //4.按照Key进行排序
    kvRDD.sortByKey()
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}
