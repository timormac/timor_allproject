package atguigu.core.pratice

import com.atguigu.practice.hanlder.SingleJumpHandler
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object SingleJumpRatio_04 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取用户行为数据
    val lineRDD: RDD[String] = sc.textFile("./input/user_visit_action.txt")
    lineRDD.cache()

    //4.声明访问路径
    val pages = Array(1, 2, 3, 4, 5, 6, 7)

    //5.准备过滤条件
    //5.1 单页过滤条件(1, 2, 3, 4, 5, 6)
    val fromPages: Array[Int] = pages.dropRight(1)
    //5.2 单跳过滤条件(1_2,2_3,3_4...6_7)
    val toPages: Array[Int] = pages.drop(1)
    val fromToPages: Array[String] = fromPages.zip(toPages).map { case (from, to) =>
      s"${from}_$to"
    }

    //6.计算单页访问次数:分母
    val fromPageToCount: RDD[(String, Int)] = SingleJumpHandler.getSinglePageCount(lineRDD, fromPages)

    //7.计算单跳访问次数:分子
    val singleJumpPageCount: RDD[(String, Int)] = SingleJumpHandler.getSingleJumpCount(lineRDD, fromToPages)

    //8.转换singleJumpPageCount数据结构(1_2,50) ==> (1,(1_2,50))
    val fromPageToSingleJumpPageCount: RDD[(String, (String, Int))] = singleJumpPageCount.map { case (fromToPage, count) =>
      (fromToPage.split("_")(0), (fromToPage, count))
    }

    //9.将分子分母进行JOIN(RDD[(String, ((String, Int), Int))])并计算最终结果
    val result: RDD[(String, Double)] = fromPageToSingleJumpPageCount.join(fromPageToCount)
      .map { case (fromPage, ((fromToPage, count1), count2)) =>
        (fromToPage, count1 / count2.toDouble)
      }

    //10.打印
    result.sortByKey()
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()
  }

}


class CategoryTop10Accumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  private var map: mutable.HashMap[String, Int] = new mutable.HashMap()

  //累加器判空
  override def isZero: Boolean = map.isEmpty

  //复制一个累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = new CategoryTop10Accumulator

  //重置累加器
  override def reset(): Unit = map.clear()

  //分区内部添加数据
  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0) + 1
  }

  //分区间合并数据
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other.value.foreach { case (actionCategory, count) =>
      map(actionCategory) = map.getOrElse(actionCategory, 0) + count
    }
  }

  //返回值
  override def value: mutable.HashMap[String, Int] = map
}