package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_Transform_Test {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    //4.转换为字符串打印
    valueRDD.collect().foreach(x => println(x + ""))
    //    valueRDD.collect().foreach(println(_ + ""))
    //    valueRDD.collect().foreach(y => println(x => x + ""))

    //关闭连接
    sc.stop()

  }

}