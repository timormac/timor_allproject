package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//zeroValue:指的是给每个分区中每种Key分配一次初始值
//seqOp:指的是分区内对于相同的Key对应的Value进行迭代计算(预聚合)
//combOp:指的是分区间对于相同的Key对应的Value进行迭代计算
object Spark02_Transform_K_V_AggregateByKey {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 3), ("a", 2), ("c", 4),
      ("b", 3), ("c", 6), ("c", 8)
    ), 2)

    //4.取出每个分区内相同key的最大值然后分区间相加
    //    kvRDD.aggregateByKey(0)((x, y) => math.max(x, y), (a, b) => a + b)
    //      .collect()
    //      .foreach(println)

    //5.WordCount
    kvRDD.aggregateByKey(1000)(_ + _, _ + _)
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}
