package atguigu.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Persist_Cache {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    //4.扩大两倍
    val doubleValueRDD: RDD[Int] = valueRDD.map(x => {
      println(x)
      x * 2
    })

    //    doubleValueRDD.persist()
    //Cache将任务中获取的RDD的数据持久化到内存,RDD复用时不需要从头计算
    //Cache不会切断血缘关系
    doubleValueRDD.cache()
    println(doubleValueRDD.toDebugString)

    //5.打印
    doubleValueRDD.collect().foreach(println)

    //6.保存到文件
    doubleValueRDD.saveAsTextFile("./outCache1")

    //关闭连接
    sc.stop()

  }

}
