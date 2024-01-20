package atguigu.streaming.app

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirectApiTest01 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("KafkaReceiverApiTest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.使用DirectAPI消费Kafka数据创建流
    val kafkaParams: Map[String, String] = Map[String, String](
      "zookeeper.connect" -> "project1:2181,project2:2181,project3:2181",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "project1:9092,project2:9092,project3:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "kafkaDemo")

    val kafkaStream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String]  (Set("topicName"), kafkaParams)
    )
    val DStream: DStream[String] = kafkaStream.map(_.value())

    //4.计算WordCount并打印
    DStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
