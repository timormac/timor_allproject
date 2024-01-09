package atguigu.streaming.day03

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Spark01_WordCount_State {

  def updateFunc(seq: Seq[Int], state: Option[Int]): Option[Int] = {
    //计算当前批次数据
    val currentSum: Int = seq.sum
    //获取之前批次的数据
    val lastSum: Int = state.getOrElse(0)
    //返回结果
    Some(currentSum + lastSum)
  }

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("./ck2")

    //3.接收数据创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.压平
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    //5.将单词映射为元组
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //6.使用updateStateByKey实现累积的WordCount
    val wordToCount: DStream[(String, Int)] = wordToOneDStream.updateStateByKey(updateFunc)

    //7.打印
    wordToCount.print()

    //8.启动
    ssc.start()
    ssc.awaitTermination()

  }

}
