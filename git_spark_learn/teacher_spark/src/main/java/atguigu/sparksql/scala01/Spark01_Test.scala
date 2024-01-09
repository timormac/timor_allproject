package atguigu.sparksql.scala01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Spark01_Test {

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

    //5.打印
    df.show()

    //6.使用SQL风格
    //6.1 建视图
    df.createTempView("people")
    //6.2 执行SQL
    spark.sql(
      """
        |select
        |    *
        |from
        |    people
      """.stripMargin).show()

    //7.DSL风格
    df.select("name").show()

    //8.将DF转换为RDD
    val rdd: RDD[Row] = df.rdd
    rdd.collect().foreach(println)

    //9.将RDD转换为DF
    rdd.map(row => {
      People(row.getLong(0), row.getString(1))
    }).toDF().show()

    //10.关闭连接
    spark.stop()

  }

}

case class People(age: Long, name: String)
