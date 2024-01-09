package atguigu.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.读取数据: ==> hello spark
    //Path: 可以是目录,也可以是具体的文件
    val line: RDD[String] = sc.textFile(args(0))

    //4.扁平化操作   {hello spark}  ==>  hello   spark
    val word: RDD[String] = line.flatMap(_.split(" "))

    //5.将每一个单词转换为元组 hello  ==>  (hello,1)
    val wordToOne: RDD[(String, Int)] = word.map((_, 1))

    //6.按照key做聚合操作  (hello,1)  ==>  (hello,sum)
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => x + y)

    //7.打印
    //    wordToCount.collect().foreach(println)

    //7.将数据输出到文件
    val unit: Unit = wordToCount.saveAsTextFile(args(1))
    val tuples: Array[(String, Int)] = wordToCount.collect()
    val unit1: Unit = wordToCount.foreach(println)

    //sc.textFile(" ").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)

    //8.关闭SC
    sc.stop()

  }
}
