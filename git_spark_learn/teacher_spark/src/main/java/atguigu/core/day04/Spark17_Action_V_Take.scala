package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Action_V_Take {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[String] = sc.makeRDD(Array("1", "2", "3", "4"), 2)

    //4.取出当前RDD中的前几个元素
    println(valueRDD.take(2))

    //关闭连接
    sc.stop()

  }

}
