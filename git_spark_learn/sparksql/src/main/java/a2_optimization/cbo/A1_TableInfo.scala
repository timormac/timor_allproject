package a2_optimization.cbo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Author Timor
 * @Date 2024/3/3 10:09
 * @Version 1.0
 */
object A1_TableInfo {
  def main( args: Array[String] ): Unit = {
    //TODO 要打包提交集群执行，注释掉setMaster
    val conf: SparkConf = new SparkConf().setAppName("CBOTunning").setMaster("local[*]")

    val session: SparkSession = SparkSession
      .builder()
      .config(conf)
      //指定连接hive,默认从resource配置文件读取
      .enableHiveSupport()
      .getOrCreate()

    val frame: DataFrame = session.sql("show databases")
    frame.show()

    val rdd: RDD[Row] = frame.rdd

    session.implicits


//    val f1: DataFrame = session.sql( "select * from personal_db_demo.score " )
//    f1.show()

    session.stop()

  }

  def AnalyzeTableAndColumn( sparkSession: SparkSession, tableName: String, columnListStr: String ): Unit = {
    //TODO 查看 表级别 信息
    println("=========================================查看" + tableName + "表级别 信息========================================")
    sparkSession.sql("DESC FORMATTED " + tableName).show(100)
    //TODO 统计 表级别 信息
    println("=========================================统计 " + tableName + "表级别 信息========================================")
    sparkSession.sql("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS").show()
    //TODO 再查看 表级别 信息
    println("======================================查看统计后 " + tableName + "表级别 信息======================================")
    sparkSession.sql("DESC FORMATTED " + tableName).show(100)


    //TODO 查看 列级别 信息
    println("=========================================查看 " + tableName + "表的" + columnListStr + "列级别 信息========================================")
    val columns: Array[String] = columnListStr.split(",")
    for (column <- columns) {
      sparkSession.sql("DESC FORMATTED " + tableName + " " + column).show()
    }
    //TODO 统计 列级别 信息
    println("=========================================统计 " + tableName + "表的" + columnListStr + "列级别 信息========================================")
    sparkSession.sql(
      s"""
         |ANALYZE TABLE ${tableName}
         |COMPUTE STATISTICS
         |FOR COLUMNS $columnListStr
      """.stripMargin).show()
    //TODO 再查看 列级别 信息
    println("======================================查看统计后 " + tableName + "表的" + columnListStr + "列级别 信息======================================")
    for (column <- columns) {
      sparkSession.sql("DESC FORMATTED " + tableName + " " + column).show()
    }
  }

}
