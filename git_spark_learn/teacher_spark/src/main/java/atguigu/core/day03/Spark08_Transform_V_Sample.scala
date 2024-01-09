package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Transform_V_Sample {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)

    //4.对数据进行抽样
    value.sample(false, 0.3)
      .collect()
      .foreach(println)

    value.sample(true, 0.3)
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}
