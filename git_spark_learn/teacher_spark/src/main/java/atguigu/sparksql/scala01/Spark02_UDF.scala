package atguigu.sparksql.scala01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark02_UDF {

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
    spark.udf.register("addAge", (age: Long) => age + 5)

    //6.创建视图
    df.createTempView("people")

    //7.在SQL中使用自定义的函数
    spark.sql("select name,addAge(age) from people").show()

    //8.关闭资源
    spark.stop()
  }

}
