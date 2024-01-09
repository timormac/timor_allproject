package atguigu.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Spark04_WordCount_Transform {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount_RDD").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取端口数据创建流
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //Driver,全局一次
    println(s"1111111111111${Thread.currentThread().getName}")

    //4.使用Transform算子计算WordCount并打印
    val wordToCountDStream: DStream[(String, Int)] = lineDStream.transform(rdd => {

      //Driver,一个批次一次
      println(s"222222222222${Thread.currentThread().getName}")

      rdd.flatMap(_.split(" "))
        .map(x => {
          //Executor,跟单词的个数相同
          println(s"3333333333333${Thread.currentThread().getName}")
          (x, 1)
        })
        .reduceByKey(_ + _)
    })
    wordToCountDStream.print()

    //启动任务
    ssc.start()
    //阻塞
    ssc.awaitTermination()


  }

}
