package atguigu.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Serialization_Test {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[String] = sc.makeRDD(Array("hello", "atguigu/core", "scala", "spark"), 2)

    //4.创建Search对象
    val search = new Search("a")

    //5.过滤出包含"a"
    //    val filterRDD: RDD[String] = search.getMatch1(valueRDD)
    val filterRDD: RDD[String] = search.getMatch2(valueRDD)
    filterRDD.collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}


class Search(query: String) {

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 函数序列化案例
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
    //    rdd.filter(this.isMatch)
  }

  // 属性序列化案例
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    val str: String = query
    rdd.filter(x => x.contains(str))
    //    rdd.filter(x => x.contains(this.query))
  }
}