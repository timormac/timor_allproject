package atguigu.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark05_WordCount_Join {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark05_WordCount_Join").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.接收数据创建DStream
    val lineDStream1: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val lineDStream2: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 8888)

    //4.压平
    val wordDStream1: DStream[String] = lineDStream1.flatMap(_.split(" "))
    val wordDStream2: DStream[String] = lineDStream2.flatMap(_.split(" "))

    //5.将单词映射为元组
    val wordToOneDStream1: DStream[(String, Int)] = wordDStream1.map((_, 1))
    val wordToOneDStream2: DStream[(String, Int)] = wordDStream2.map((_, 1))

    //6.JOIN
    val result: DStream[(String, (Int, Int))] = wordToOneDStream1.join(wordToOneDStream2)

    //7.打印
    result.print()

    //8.开启任务
    ssc.start()
    ssc.awaitTermination() //将主线程阻塞,主线程不退出

  }

}