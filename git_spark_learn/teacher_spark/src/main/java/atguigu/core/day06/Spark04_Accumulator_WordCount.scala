package atguigu.core.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Accumulator_WordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取文件数据
    val lineRDD: RDD[String] = sc.textFile("./input/1.txt")

    //4.压平操作
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))


    //关闭连接
    sc.stop()

  }

}

class WordCountAccu extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  private val map = new mutable.HashMap[String, Int]()

  //判空
  override def isZero: Boolean = map.isEmpty

  //复制
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = new WordCountAccu

  //重置
  override def reset(): Unit = map.clear()

  //区内累加数据
  override def add(v: String): Unit = {

    map(v) = map.getOrElse(v, 0) + 1

  }

  //区间合并数据
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other.value.foreach { case (word, count) =>
      map(word) = map.getOrElse(word, 0) + count
    }
  }

  //返回值
  override def value: mutable.HashMap[String, Int] = map
}
