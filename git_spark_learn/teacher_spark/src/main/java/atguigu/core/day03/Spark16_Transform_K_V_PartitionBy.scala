package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark16_Transform_K_V_PartitionBy {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 1), ("c", 1), ("d", 1)), 2)

    //4.打印当前分区分配
    kvRDD.mapPartitionsWithIndex((index, iter) => {
      iter.map(x => (index, x))
    }).collect()
      .foreach(println)

    //5.重新分区
    kvRDD.partitionBy(new HashPartitioner(2))
      .mapPartitionsWithIndex((index, iter) => {
        iter.map(x => (index, x))
      }).collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}