package atguigu.sparksql.scala01

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark04_UDAF_02 {

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

    //5.转换为DS
    val ds: Dataset[People] = df.as[People]

    //6.根据自定UDAF函数创建列
    val avg0 = new MyAvg02
    val column: TypedColumn[People, Double] = avg0.toColumn

    //7.使用
    ds.select(column).show()

    //8.关闭资源
    spark.stop()
  }
}

case class AvgBuffer(var sum: Long, var count: Int)

class MyAvg02 extends Aggregator[People, AvgBuffer, Double] {

  //缓存数据的初始化
  override def zero: AvgBuffer = AvgBuffer(0L, 0)

  //区内累加数据
  override def reduce(b: AvgBuffer, a: People): AvgBuffer = {
    b.sum += a.age
    b.count += 1
    b
  }

  //区间合并数据
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //最终计算的函数
  override def finish(reduction: AvgBuffer): Double = {
    Math.round(reduction.sum * 100D / reduction.count) / 100D
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
