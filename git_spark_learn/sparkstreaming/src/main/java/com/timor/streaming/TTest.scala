package com.timor.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Title: TTest
 * @Package: com.timor.streaming
 * @Description:
 * @Author: lpc
 * @Date: 2024/1/23 10:42
 * @Version:1.0
 */
object TTest {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("KafkaReceiverApiTest").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.使用DirectAPI消费Kafka数据创建流
    val kafkaParams: Map[String, String] = Map[String, String](
      "zookeeper.connect" -> "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata0523_1")

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("topicName"), kafkaParams)
    )


    val map: DStream[String] = kafkaStream.map( _.value())

    //map.transform()

    //注意当DStream的类型时String,没有join方法，当DStream的类型是二元组时有join方法
    val tuple: DStream[(String, Int)] = map.map((_, 1))
    val join: DStream[(String, (Int, Int))] = tuple.join(tuple)



  }

}
