package atguigu.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Practice_GroupBy {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取apache.log
    val logRDD: RDD[String] = sc.textFile("./input/apache.log")

    //4.从服务器日志数据apache.log中获取每个时间段（天）访问量。
    val dateToOne: RDD[(String, Int)] = logRDD.map(log => {
      //a.按照空格切分并取第4为时间字段:17/05/2015:10:05:03
      val timeStr: String = log.split(" ")(3)
      //b.提取数据中的天
      val dateStr: String = timeStr.split(":")(0)
      //c.封装为(天,1)
      (dateStr, 1)
    })

    //5.计算每天的总访问量
    val dateToIter: RDD[(String, Iterable[(String, Int)])] = dateToOne.groupBy(_._1)
    dateToIter.map { case (date, iter) =>
      (date, iter.size)
    }.collect()
      .foreach(println)

    //关闭连接
    sc.stop()

  }

}
