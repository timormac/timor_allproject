package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Transform_K_V_Test03 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", -3), ("a", -2), ("c", -4),
      ("b", -3), ("c", -6), ("c", -8)
    ), 2)

    //4.取出每个分区内相同key的最大值然后分区间相加
    kvRDD.combineByKey((x: Int) => x,
      (x: Int, y: Int) => math.max(x, y),
      (a: Int, b: Int) => a + b)
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}
