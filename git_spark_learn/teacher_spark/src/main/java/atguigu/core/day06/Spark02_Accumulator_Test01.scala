package atguigu.core.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Accumulator_Test01 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    //4.定义变量sum
    //    var sum: Int = 0
    //注册累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")

    //5.扩大两倍
    val doubleValueRDD: RDD[Int] = valueRDD.map(x => {
      //      sum += x
      sum.add(x)
      x * 2
    })

    //6.打印数据并打印sum值
    doubleValueRDD.collect().foreach(println)
    println(sum.value)

    //关闭连接
    sc.stop()

  }

}
