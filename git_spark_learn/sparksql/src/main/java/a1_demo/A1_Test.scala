package a1_demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author Timor
 * @Date 2024/2/23 11:31
 * @Version 1.0
 */
object A1_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("lpc01").setMaster("local[*]")

    val session: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val df: DataFrame = session.read.json("./input/people.json")

    //??filter源码看不懂，要传什么，不知道
    df.filter( df("date") > "10")





    //使用SQL风格
    df.createTempView("people")



    val frame: DataFrame = session.sql(
      """
        |select
        |    *
        |from
        |    people
      """.stripMargin)
    frame




  }

}
