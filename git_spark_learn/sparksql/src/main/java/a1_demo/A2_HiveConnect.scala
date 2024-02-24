package a1_demo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Author Timor
 * @Date 2024/2/23 11:58
 * @Version 1.0
 */
object A2_HiveConnect {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("lpc01").setMaster("local[*]")

    val session: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val frame: DataFrame = session.sql("show tables")
    frame.show()

    val rdd: RDD[Row] = frame.rdd

    session.implicits


    val f1: DataFrame = session.sql( "select * from personal_db_demo.score " )

    f1.show()

    session.stop()



  }

}
