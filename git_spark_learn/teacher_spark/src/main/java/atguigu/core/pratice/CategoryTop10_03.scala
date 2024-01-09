package atguigu.core.pratice

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object CategoryTop10_03 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取用户行为数据
    val start: Long = System.currentTimeMillis()
    val lineRDD: RDD[String] = sc.textFile("./input/user_visit_action.txt")
    lineRDD.cache()

    //4.使用累计器计算各个品类点击、订单以及支付数量
    val accu = new CategoryTop10Accumulator
    sc.register(accu)
    lineRDD.foreach(line => {

      //分割数据
      val arr: Array[String] = line.split("_")

      //判断是否为点击数据
      if (arr(6) != "-1") {
        accu.add(s"click_${arr(6)}")
      } else if (arr(8) != "null") {
        //订单数据
        arr(8).split(",")
          .foreach(category =>
            accu.add(s"order_$category")
          )
      } else if (arr(10) != "null") {
        //支付数据
        arr(10).split(",")
          .foreach(category =>
            accu.add(s"pay_$category")
          )
      }
    })

    //5.取出累加器中的数据Map[(click_16,50),(order_15,45)...]
    val actionCategoryToCountMap: mutable.HashMap[String, Int] = accu.value

    //6.按照品类分组Map[(1,Map[(click_1,50),(pay_1,40),(order_1,45)])]
    val categoryToMap: Map[String, mutable.HashMap[String, Int]] = actionCategoryToCountMap.groupBy(_._1.split("_")(1))

    //7.转换数据结构(1,Map[(click_1,50),(pay_1,40),(order_1,45)])=>(1,(50,45,40))
    val categoryToClickOrderPayCount: Map[String, (Int, Int, Int)] = categoryToMap.map { case (category, map) =>
      (category, (map.getOrElse(s"click_$category", 0), map.getOrElse(s"order_$category", 0), map.getOrElse(s"pay_$category", 0)))
    }

    //8.排序并取前十
    val top10CategoryToCount: List[(String, (Int, Int, Int))] = categoryToClickOrderPayCount
      .toList
      .sortBy(_._2)
      .reverse
      .take(10)

    //9.需求一:打印
    top10CategoryToCount.foreach(println)

    //需求二
    //10.取出前十品类
    val top10Category: List[String] = top10CategoryToCount.map(_._1)

    //11.过滤原始数据集,将我们需要的前10品类过滤出来
    val filterRDD: RDD[String] = lineRDD.filter(line => {
      //分割数据
      val arr: Array[String] = line.split("_")
      //根据需求一结果进行过滤
      top10Category.contains(arr(6))
    })

    //12.将数据转换结构 line==>((category,sessionId),1)
    val categorySessionToOne: RDD[((String, String), Int)] = filterRDD.map(line => {
      //分割数据
      val arr: Array[String] = line.split("_")
      ((arr(6), arr(2)), 1)
    })

    //13.计算总数
    val categorySessionToCount: RDD[((String, String), Int)] = categorySessionToOne.reduceByKey(_ + _)

    //14.调整数据结构,将品类提取出来 ((category, session), count) => (category, (session, count))
    val categoryToSessionCount: RDD[(String, (String, Int))] = categorySessionToCount.map { case ((category, session), count) =>
      (category, (session, count))
    }

    //15.按照品类分组
    val categoryToSessionCountIter: RDD[(String, Iterable[(String, Int)])] = categoryToSessionCount.groupByKey()

    //16.对Value进行排序并取前十
    categoryToSessionCountIter.mapValues(iter => {
      iter.toList.sortWith(_._2 > _._2).take(10)
    }).flatMapValues(list => list)
      .collect()
      .foreach(println)

    //关闭连接
    sc.stop()

    val stop: Long = System.currentTimeMillis()

    println(stop - start)

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