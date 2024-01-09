package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Transform_K_V_Test04 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD: RDD[(String, Array[Int])] = sc.makeRDD(List(("a", Array(1, 2, 3)), ("b", Array(1, 2))), 2)

    //4.将元组的第二个字段炸开
    //    val result: RDD[(String, Int)] = kvRDD.flatMap { case (value, arr) =>
    //      arr.map(x => (value, x))
    //    }
    val result: RDD[(String, Int)] = kvRDD.flatMapValues(x => x)

    //5.打印
    result.collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}
