package com.timor.streaming

import com.timor.utils.A0_Configs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author Timor
 * @Date 2024/1/22 16:13
 * @Version 1.0
 */
object A0_KafkaStream {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("KafkaReceiverApiTest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.使用DirectAPI消费Kafka数据创建流
    val kafkaParams: Map[String, String] = Map[String, String](
      // "zookeeper.connect" ->  A0_Configs.ZOOKEEPER_SERVERS,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> A0_Configs.KAFKA_SERVERS ,
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata0523_1",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName
    )



    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("terminal_produce"), kafkaParams)
    )
    val DStream: DStream[String] = kafkaStream.map(_.value())
    DStream.print()

    ssc.start()
    ssc.awaitTermination()


  }


}