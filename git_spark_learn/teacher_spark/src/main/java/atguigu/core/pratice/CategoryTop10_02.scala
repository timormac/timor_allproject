package atguigu.core.pratice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object CategoryTop10_02 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取用户行为数据
    val start: Long = System.currentTimeMillis()
    val lineRDD: RDD[String] = sc.textFile("./input/user_visit_action.txt")

    //4.line=>(1,(1,0,0)) or (1,(0,1,0)) or (1,(0,0,1))
    val categoryToClickOrderPayOne: RDD[(String, (Int, Int, Int))] = lineRDD.flatMap(line => {

      //按照"_"分割数据
      val arr: Array[String] = line.split("_")

      //判断是否为点击数据
      if (arr(6).toInt != -1) {
        List((arr(6), (1, 0, 0)))

        //判断是否为订单数据
      } else if (arr(8) != "null") {
        arr(8).split(",").map(category => (category, (0, 1, 0)))

        //判断是否为支付数据
      } else if (arr(10) != "null") {
        arr(10).split(",").map(category => (category, (0, 0, 1)))
      } else {
        //搜索数据
        Nil
      }
    })

    //5.计算各个品类点击、订单以及支付的总次数
    val categoryToClickOrderPayCount: RDD[(String, (Int, Int, Int))] = categoryToClickOrderPayOne.reduceByKey((x, y) =>
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    )

    //6.按照第二位进行倒序比较取前十
    categoryToClickOrderPayCount
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach(println)

    //关闭连接
    sc.stop()

    val stop: Long = System.currentTimeMillis()

    println(stop - start)

  }

}
