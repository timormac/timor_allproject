package atguigu.sparksql.scala01

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark05_UDAF_03 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_Test").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    //3.导入隐式转换
    import spark.implicits._

    //4.读取JSON数据创建DF
    val df: DataFrame = spark.read.json("./input/people.json")

    //5.注册UDF函数
    spark.udf.register("myAvg", functions.udaf(new MyAvg03))

    //6.创建视图
    df.createTempView("people")

    //7.在SQL中使用自定义的函数
    spark.sql("select myAvg(age) from people").show()

    //8.关闭资源
    spark.stop()
  }

}

class MyAvg03 extends Aggregator[Long, AvgBuffer, Double] {

  //初始化
  override def zero: AvgBuffer = AvgBuffer(0L, 0)

  //区内累加数据
  override def reduce(b: AvgBuffer, a: Long): AvgBuffer = {
    b.sum += a
    b.count += 1
    b
  }

  //区间合并数据
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //最终计算
  override def finish(reduction: AvgBuffer): Double = {
    Math.round(reduction.sum * 100D / reduction.count) / 100D
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}