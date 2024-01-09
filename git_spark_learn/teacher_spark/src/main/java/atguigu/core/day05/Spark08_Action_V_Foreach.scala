package atguigu.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Action_V_Foreach {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    //4.打印
    kvRDD.foreach(println)
    println("*************")
    kvRDD.foreach(println)
    println("*************")
    kvRDD.foreach(println)
    println("*************")
    kvRDD.foreach(println)

    //写库操作使用foreachPartition代替foreach,减少连接的创建与释放
    kvRDD.foreachPartition(iter => {
      //a.创建连接
      //b.遍历操作数据
      iter.foreach(println)
      //c.释放连接
    })

    //关闭连接
    sc.stop()

  }

}
