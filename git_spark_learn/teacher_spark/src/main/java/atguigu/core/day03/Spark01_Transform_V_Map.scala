package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Transform_V_Map {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD并将每一个元素扩大两倍
    //    val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)
    //    value.map((x: Int) => x + "").collect().foreach(println)

    //4.从服务器日志数据apache.log中获取用户请求URL资源路径
    val lineRDD: RDD[String] = sc.textFile("./input/apache.log")

    val urlRDD: RDD[(String, String)] = lineRDD.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(3), arr(6))
    })

    //5.打印
    urlRDD.collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}
