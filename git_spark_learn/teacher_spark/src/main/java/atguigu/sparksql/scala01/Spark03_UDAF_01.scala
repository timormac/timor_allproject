package atguigu.sparksql.scala01

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Spark03_UDAF_01 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_Test").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    //3.导入隐式转换

    //4.读取JSON数据创建DF
    val df: DataFrame = spark.read.json("./input/people.json")

    //5.注册UDAF函数
    spark.udf.register("myAvg", new MyAvg01)

    //6.SQL风格
    df.createTempView("people")
    spark.sql("select myAvg(age) from people")
      .show()

    //7.关闭资源
    spark.stop()

  }

}

class MyAvg01 extends UserDefinedAggregateFunction {

  //定义输入数据类型
  override def inputSchema: StructType = StructType(Array(StructField("input", LongType)))

  //定义中间数据类型
  override def bufferSchema: StructType = StructType(Array(
    StructField("sum", LongType),
    StructField("count", IntegerType)))

  //定义输出数据类型
  override def dataType: DataType = DoubleType

  //定义当前函数类型
  override def deterministic: Boolean = true

  //给缓冲数据做初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0
  }

  //区内累加数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getInt(1) + 1
  }

  //区间合并数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  //最终计算的函数
  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0) / buffer.getInt(1).toDouble
  }
}