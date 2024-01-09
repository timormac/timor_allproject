package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Transform_V_FlatMap {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    //    val listRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4, 5)), 1)
    val anyRDD: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)), 1)

    //4.扁平操作 ==> RDD[Int](1,2,3,4,5)
    //    listRDD.flatMap(list => list).collect().foreach(println)
    //    anyRDD.flatMap(data => {
    //      data match {
    //        case list: List[_] => list
    //        case value: Int => List(value)
    //      }
    //    })

    anyRDD.flatMap {
      case list: List[_] => list
      case value: Int => List(value)
    }.collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}
