package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Transform_V_Glom {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)

    //4.将每个分区的所有元素放入一个数组中进行返回
    //    val arrRDD: RDD[Array[Int]] = value.glom()
    //    //5.打印
    //    arrRDD.collect().foreach(x => {
    //      x.foreach(print)
    //      println
    //    })

    //6.计算所有分区最大值
    value.glom().map(_.max)
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}
