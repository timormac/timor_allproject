package atguigu.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark01_WordCount_Socket {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.接收数据创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.压平
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    //5.将单词映射为元组
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //6.计算单词总数
    val wordToCountDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)

    //7.打印
    wordToCountDStream.print()

    //8.开启任务
    ssc.start()
    ssc.awaitTermination() //将主线程阻塞,主线程不退出

  }

}