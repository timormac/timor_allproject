package atguigu.streaming.day03

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark01_WordCount_Window01 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.接收数据创建DStream
    //    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val kafkaParam: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata0523_2",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val lineDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaParam))

    val lineByWindowDStream: DStream[ConsumerRecord[String, String]] = lineDStream.window(Seconds(12), Seconds(6))

    lineByWindowDStream.flatMap(_.value().split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //    //4.压平
    //    val wordDStream: DStream[String] = lineByWindowDStream.flatMap(_.split(" "))
    //
    //    //5.将单词映射为元组
    //    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    //
    //    //7.计算总和并打印
    //    val wordToCountDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)
    //    wordToCountDStream.print()

    //8.启动
    ssc.start()
    ssc.awaitTermination()

  }

}
