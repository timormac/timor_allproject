package atguigu.sparksql.scala01

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Spark07_Write {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_Test").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val df: DataFrame = spark.read.json("")

    //3.保存数据方式一
    df.write.json("")
    df.write.csv("")
    df.write.orc("")
    df.write.parquet("")
    df.write.jdbc("", "", new Properties())

    //4.保存数据方式二
    df.write.format("json").save("")
    df.write.format("csv").save("")
    df.write.format("orc").save("")
    df.write.format("parquet").save("")
    df.write.format("jdbc").option("", "").option("", "").save()
    df.write.save("") //parquet格式

    //5.保存方式
    df.write.mode(SaveMode.Append).parquet("")
    df.write.mode(SaveMode.Overwrite).format("json").save("")
    df.write.mode(SaveMode.ErrorIfExists).parquet("")
    df.write.mode(SaveMode.Ignore).parquet("")

    //5.关闭资源
    spark.stop()
  }

}
