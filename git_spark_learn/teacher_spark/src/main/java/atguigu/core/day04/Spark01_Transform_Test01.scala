package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Transform_Test01 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[String] = sc.makeRDD(Array("a", "b", "c", "d"), 2)

    //4.使其一个分区的数据转变为一个String
    //(Array("a","b","c","d"),2)=>("ab","cd")
    valueRDD.mapPartitions(iter => {
      Iterator(iter.mkString(""))
    }).collect().foreach(println)

    println("*******************")

    valueRDD.glom().map(_.mkString(""))
      .collect().foreach(println)

    println("*******************")

    valueRDD.mapPartitionsWithIndex { case (_, iter) =>
      Iterator(iter.mkString(""))
    }.collect().foreach(println)

    println("*******************")

    val indexToValueRDD: RDD[(Int, String)] = valueRDD.mapPartitionsWithIndex { case (index, iter) =>
      iter.map(x => (index, x))
    }

    val indexToValueIter: RDD[(Int, Iterable[String])] = indexToValueRDD.groupByKey()

    indexToValueIter.map(x => x._2.mkString(""))
      .collect().foreach(println)

    println("*******************")

    val indexToValueRDD2: RDD[(Int, String)] = valueRDD.mapPartitionsWithIndex { case (index, iter) =>
      iter.map(x => (index, x))
    }

    val indexToStr: RDD[(Int, String)] = indexToValueRDD2.reduceByKey(_ + _)

    indexToStr.map(_._2)
      .collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}
