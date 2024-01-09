package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Transform_V_GroupBy {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 5, 6, 5, 6, 8, 9, 1), 2)

    //4.按照奇数偶数进行分组
    //    val intToIter: RDD[(Int, Iterable[Int])] = value.groupBy(x => x % 2)
    //
    //    //5.打印数据
    //    intToIter.collect().foreach(println)

    //6.计算WordCount
    value.groupBy(x => x).map { case (value, iter) =>
      (value, iter.size)
    }.collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}
