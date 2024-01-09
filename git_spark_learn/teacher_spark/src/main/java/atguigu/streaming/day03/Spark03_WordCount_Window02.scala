package atguigu.streaming.day03

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark03_WordCount_Window02 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("./ck3")

    //3.接收数据创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.压平
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    //5.将单词映射为元组
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //7.计算总和并打印
    val wordToCountDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      (a: Int, b: Int) => a - b,
      Seconds(6),
      Seconds(3),
      new HashPartitioner(2),
      (x: (String, Int)) => x._2 > 0)

    wordToCountDStream.print()

    //8.启动
    ssc.start()
    ssc.awaitTermination()

  }

}
