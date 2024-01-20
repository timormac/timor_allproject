package atguigu.streaming.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaReceiverApiTest {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("KafkaReceiverApiTest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

//    //3.使用ReceiverAPI消费Kafka数据创建流
//    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,
//      "hadoop102:2181,hadoop103:2181,hadoop104:2181",
//      "bigdata0523",
//      Map[String, Int]("test" -> 2))
//
//    //4.计算WordCount并打印
//    kafkaDStream.flatMap(_._2.split(" "))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//      .print()

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
