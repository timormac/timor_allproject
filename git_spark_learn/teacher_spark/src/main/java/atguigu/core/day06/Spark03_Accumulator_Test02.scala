package atguigu.core.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Accumulator_Test02 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.创建RDD
    val valueRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    //4.注册累加器
    val accu = new MyAccu
    sc.register(accu, "sum")

    //5.扩大两倍并给累加器累加值
    //注意:累加器不要写在转换算子里面,转换算子可能被多次执行
    val doubleValueRDD: RDD[Int] = valueRDD.map(x => {
      accu.add(x)
      x * 2
    })

    //6.打印数据并打印sum值
    doubleValueRDD.collect().foreach(println)
    doubleValueRDD.saveAsTextFile("./out")
    println(accu.value)

    //关闭连接
    sc.stop()

  }

}

class MyAccu extends AccumulatorV2[Int, Int] {

  private var sum: Int = 0

  //判空
  override def isZero: Boolean = sum == 0

  //复制累加器
  override def copy(): AccumulatorV2[Int, Int] = new MyAccu

  //重置累加器
  override def reset(): Unit = sum = 0

  //分区内累加数据
  override def add(v: Int): Unit = sum += v

  //分区间累加数据
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    this.sum += other.value
  }

  //返回值
  override def value: Int = sum
}