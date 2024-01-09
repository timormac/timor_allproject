package atguigu.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Practice_SortByKey {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val userRDD: RDD[(User, Int)] = sc.makeRDD(Array((new User(1001, "zhangsan"), 1),
      (new User(1003, "lisi"), 1),
      (new User(1002, "wangwu"), 1)), 2)

    //4.按照Key排序
    userRDD.sortByKey()
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}


case class User(val id: Int, val name: String) extends Comparable[User] {

  override def compareTo(o: User): Int = {

    if (this.id > o.id) {
      1
    } else {
      -1
    }

  }
}