package atguigu.sparksql.scala01

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark06_Read {

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

    //4.读取数据方式一
    spark.read.json("")
    spark.read.csv("")
    spark.read.orc("")
    spark.read.parquet("")
    spark.read.jdbc("", "", new Properties())

    //5.读取数据方式二
    spark.read.format("json").load("")
    spark.read.format("csv").load("")
    spark.read.format("orc").load("")
    spark.read.format("parquet").load("")
    spark.read.format("jdbc").option("", "").option("", "").load()
    spark.read.load("") //读取的为parquet格式

    //8.关闭资源
    spark.stop()
  }

}
