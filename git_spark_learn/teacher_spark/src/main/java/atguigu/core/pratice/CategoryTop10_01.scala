package atguigu.core.pratice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object CategoryTop10_01 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取用户行为数据
    val start: Long = System.currentTimeMillis()
    val lineRDD: RDD[String] = sc.textFile("./input/user_visit_action.txt")
    lineRDD.cache()

    //4.1 计算各个品类点击总数
    //4.1.1 过滤出点击数据
    val clickRDD: RDD[String] = lineRDD.filter(line => {
      val arr: Array[String] = line.split("_")
      arr(6).toInt != -1
    })

    //4.1.2 将数据转换为 (categoryId,1)
    val clickCategoryToOne: RDD[(String, Int)] = clickRDD.map(line => {
      val arr: Array[String] = line.split("_")
      (arr(6), 1)
    })

    //4.1.3 计算总数
    val clickCategoryToCount: RDD[(String, Int)] = clickCategoryToOne.reduceByKey(_ + _)

    //4.2 计算各个品类下单总数
    //4.2.1 过滤出订单数据
    val orderRDD: RDD[String] = lineRDD.filter(line => {
      val arr: Array[String] = line.split("_")
      arr(8) != "null"
    })

    //4.2.2 将数据转换为 (categoryId,1)
    val orderCategoryToOne: RDD[(String, Int)] = orderRDD.flatMap(line => {
      val arr: Array[String] = line.split("_")
      arr(8).split(",").map((_, 1))
    })

    //4.2.3 计算总数
    val orderCategoryToCount: RDD[(String, Int)] = orderCategoryToOne.reduceByKey(_ + _)

    //4.3 计算各个品类支付总数
    //4.3.1 过滤出订单数据
    val payRDD: RDD[String] = lineRDD.filter(line => {
      val arr: Array[String] = line.split("_")
      arr(10) != "null"
    })

    //4.3.2 将数据转换为 (categoryId,1)
    val payCategoryToOne: RDD[(String, Int)] = payRDD.flatMap(line => {
      val arr: Array[String] = line.split("_")
      arr(10).split(",").map((_, 1))
    })

    //4.3.3 计算总数
    val payCategoryToCount: RDD[(String, Int)] = payCategoryToOne.reduceByKey(_ + _)

    //5.将点击数据、订单数据、支付数据JOIN
    val clickOrderPayJoinRDD: RDD[(String, ((Int, Option[Int]), Option[Int]))] = clickCategoryToCount
      .leftOuterJoin(orderCategoryToCount)
      .leftOuterJoin(payCategoryToCount)

    val result: RDD[(String, (Int, Int, Int))] = clickOrderPayJoinRDD.map { case (category, ((click, orderOpt), payOpt)) =>
      (category, (click, orderOpt.getOrElse(0), payOpt.getOrElse(0)))
    }

    //6.排序取前十
    result.sortBy(_._2, ascending = false).take(10).foreach(println)

    //关闭连接
    sc.stop()

    val stop: Long = System.currentTimeMillis()

    println(stop - start)

  }

}
