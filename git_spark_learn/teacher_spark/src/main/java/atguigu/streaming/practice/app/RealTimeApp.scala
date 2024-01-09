package atguigu.streaming.practice.app

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.atguigu.practice.bean.Ads_log
import com.atguigu.practice.handler.{BlackListHandler, DateAreaCityAdCountHandler, LastHourAdCountHandler}
import com.atguigu.practice.utils.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.消费Kafka数据
    val properties: Properties = PropertiesUtil.load("config.properties")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(properties.getProperty("kafka.topic"), ssc)

    //4.将每行数据转换为样例类对象
    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(record => {
      //a.获取value
      val value: String = record.value()
      //b.按照" "切分
      val arr: Array[String] = value.split(" ")
      //c.封装样例类对象
      Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
    })

    //打印过滤前的数据条数
    adsLogDStream.cache()
    adsLogDStream.count().print()

    //5.根据黑名单过滤数据
    val filterAdsLogDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream, ssc.sparkContext)

    //打印过滤后的数据集条数
    filterAdsLogDStream.cache()
    filterAdsLogDStream.count().print()

    //6.计算当前批次中,当天某个用户对于某个广告的点击次数
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dateUserAdToCountDStream: DStream[((String, String, String), Int)] = filterAdsLogDStream.map(log => {
      //a.提取时间戳
      val timestamp: Long = log.timestamp
      //b.将时间戳转换为时间字符串
      val date: String = sdf.format(new Date(timestamp))
      //c.封装元组
      ((date, log.userid, log.adid), 1)
    }).reduceByKey(_ + _)

    //7.将当前批次的数据结合MySQL中已有的数据写出
    //  然后读取用户对于某个广告的单日点击总次数
    //  如果点击总次数超过100,则将该用户写入黑名单(Mysql)
    BlackListHandler.saveBlackListToMysql(dateUserAdToCountDStream)

    //8.需求二:计算每天各地区各城市各广告的点击总流量，并将其存入MySQL
    DateAreaCityAdCountHandler.saveResultToMysql(filterAdsLogDStream)

    //9.需求三:最近一小时广告点击量,按分钟展示
    //    1：List [15:50->10,15:51->25,15:52->30]
    //    2：List [15:50->10,15:51->25,15:52->30]
    //    3：List [15:50->10,15:51->25,15:52->30]
    LastHourAdCountHandler.getLastHourAdCount(filterAdsLogDStream)

    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
