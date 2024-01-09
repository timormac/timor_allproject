package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 水塘抽样算法
  * RangePartitioner
  */
object Spark12_Transform_V_SortBy {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[Int] = sc.makeRDD(Array(1, 5, 3, 4, 2, 6, 9, 8), 4)

    //4.按照数据大小进行排序
    valueRDD.sortBy(x => x)
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}