package atguigu.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_CreateRddByFile {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.从本地文件系统创建RDD
    val lineRdd: RDD[String] = sc.textFile("./input")

    //4.打印
    lineRdd.collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}
