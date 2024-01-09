package atguigu.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Action_K_V_Save {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val kvRDD: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 2), ("b", 1),
      ("a", 3), ("b", 4), ("b", 5)), 2)

    //4.保存至文件中
    kvRDD.saveAsTextFile("./outText")
    kvRDD.saveAsObjectFile("./outObj")
    kvRDD.saveAsSequenceFile("./outSeq")

    //关闭连接
    sc.stop()

  }

}
