package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Transform_K_V_Join {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 1), ("d", 1)), 2)
    val kvRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("c", 1)), 2)

    //4.JOIN
    val joinRDD: RDD[(String, (Int, Int))] = kvRDD1.join(kvRDD2)
    val value1: RDD[(String, (Int, Option[Int]))] = kvRDD1.leftOuterJoin(kvRDD2)
    val value2: RDD[(String, (Option[Int], Int))] = kvRDD1.rightOuterJoin(kvRDD2)
    val value3: RDD[(String, (Option[Int], Option[Int]))] = kvRDD1.fullOuterJoin(kvRDD2)

    //5.打印
    joinRDD.collect().foreach(println)
    println("**********")
    value1.collect().foreach(println)
    println("**********")
    value2.collect().foreach(println)
    println("**********")
    value3.collect().foreach(println)


    //关闭连接
    sc.stop()

  }

}
