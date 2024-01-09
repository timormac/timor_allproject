package atguigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Transform_V_MapPartitons {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)

    //4.将每一个元素扩大两倍
    //    value.mapPartitions(iter =>
    //      iter.map(_ * 2)
    //    ).collect().foreach(println)

    //5.获取每个数据分区的最大值
    //注意事项：
    //a.在计算过程中使用第三方存储框架时,使用mapPartitions代替map,减少连接的创建和释放
    //b.入参封装为一个迭代器,当我们需要多次遍历某个分区数据的时候,需要将迭代器转换为List
    value.mapPartitions(iter => {
      Iterator(iter.max)
    }).collect().foreach(println)

    //关闭连接
    sc.stop()

  }

}
