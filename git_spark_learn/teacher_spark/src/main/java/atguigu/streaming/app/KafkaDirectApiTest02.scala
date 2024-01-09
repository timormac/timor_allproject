package atguigu.streaming.app

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirectApiTest02 {

  def getSSC: StreamingContext = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("KafkaReceiverApiTest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("./ck1")

    //3.使用DirectAPI消费Kafka数据创建流
    val kafkaParams: Map[String, String] = Map[String, String](
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata0523_1")

    val lineDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("test"))

    //4.计算WordCount并打印
    lineDStream.flatMap(_._2.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc
  }

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", () => getSSC)

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
