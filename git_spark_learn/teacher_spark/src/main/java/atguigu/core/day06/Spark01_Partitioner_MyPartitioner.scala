package atguigu.core.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_Partitioner_MyPartitioner {


  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    //4.查看分区器
    //    println(valueRDD.partitioner)
    //    //5.打印数据
    //    valueRDD.collect().foreach(println)

    //转换数据结构
    val kvRDD: RDD[(Int, Int)] = valueRDD.map((_, 1))

    //使用自定义分区器重新分区
    val partitionRDD: RDD[(Int, Int)] = kvRDD.partitionBy(new MyPartition(3))

    //携带分区号打印数据
    partitionRDD.mapPartitionsWithIndex { case (index, iter) =>
      iter.map(x => (index, x))
    }.collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}


class MyPartition(numPar: Int) extends Partitioner {

  //返回分区数
  override def numPartitions: Int = numPar

  //对数据进行分区的方法
  override def getPartition(key: Any): Int = {
    0
  }
}