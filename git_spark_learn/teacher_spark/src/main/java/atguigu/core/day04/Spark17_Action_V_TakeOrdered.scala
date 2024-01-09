package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Action_V_TakeOrdered {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[String] = sc.makeRDD(Array("1", "2", "3", "4"), 2)

    //4.取出当前RDD中Top2
    valueRDD.sortBy(x => x, false).take(2)
    println(valueRDD.takeOrdered(2))

    //关闭连接
    sc.stop()

  }

}
