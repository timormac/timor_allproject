package atguigu.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Persist_CheckPoint {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //指定CK目录
    sc.setCheckpointDir("./ck")

    //3.创建RDD
    val valueRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    //4.扩大两倍
    val doubleValueRDD: RDD[Int] = valueRDD.map(x => {
      println(x)
      x * 2
    })

    //CheckPoint
    doubleValueRDD.checkpoint()

    //5.打印
    doubleValueRDD.collect().foreach(println)

    //6.保存到文件
    doubleValueRDD.saveAsTextFile("./outCache1")
    doubleValueRDD.saveAsTextFile("./outCache2")
    doubleValueRDD.saveAsTextFile("./outCache3")
    doubleValueRDD.saveAsTextFile("./outCache4")

    //关闭连接
    sc.stop()

  }

}
