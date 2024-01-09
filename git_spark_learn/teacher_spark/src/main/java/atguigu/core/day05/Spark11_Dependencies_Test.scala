package atguigu.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Dependencies_Test {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取文件创建RDD
    val lineRDD: RDD[String] = sc.textFile("./input/1.txt")
    println(lineRDD.dependencies)
    println("*******************")

    //4.压平操作
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("*******************")

    //5.将每一个单词映射为一个元组
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(wordToOneRDD.dependencies)
    println("*******************")

    //6.计算WordCount
    val wordToCountRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_ + _)
    println(wordToCountRDD.dependencies)
    println("*******************")

    //7.打印结果
    wordToCountRDD.collect().foreach(println)
    wordToCountRDD.saveAsTextFile("")
    wordToCountRDD.foreach(println)

    //关闭连接
    sc.stop()

  }

}
