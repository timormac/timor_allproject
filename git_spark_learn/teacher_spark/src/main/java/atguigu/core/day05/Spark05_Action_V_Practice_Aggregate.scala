package atguigu.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Action_V_Practice_Aggregate {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val rdd: RDD[String] = sc.makeRDD(Array("12", "234", "345", "4567"), 2)

    //4.写出输出结果
    // 034/043
    println(rdd.aggregate("0")((x, y) => math.max(x.length, y.length).toString, (a, b) => a + b))

    // 11
    println(rdd.aggregate("")((x, y) => math.min(x.length, y.length).toString, (a, b) => a + b))

    //关闭连接
    sc.stop()

  }

}
