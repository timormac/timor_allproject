package atguigu.streaming.practice.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.practice.bean.Ads_log
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

object LastHourAdCountHandler {

  private val sdf = new SimpleDateFormat("hh:mm")

  //需求三:最近一小时广告点击量,按分钟展示
  def getLastHourAdCount(filterAdsLogDStream: DStream[Ads_log]): Unit = {

    //1.转换数据数据结构  Ads_log  ==>  ((ad,hm),1)
    val adHmToOneDStream: DStream[((String, String), Int)] = filterAdsLogDStream.map(log => {

      val hm: String = sdf.format(new Date(log.timestamp))

      ((log.adid, hm), 1)
    })

    //2.开窗
    val adHmToOneDStreamAndWindow: DStream[((String, String), Int)] = adHmToOneDStream.window(Minutes(60))

    //3.计算点击总数     ((ad,hm),1) ==> ((ad,hm),count)
    val adHmToCountDStream: DStream[((String, String), Int)] = adHmToOneDStreamAndWindow.reduceByKey(_ + _)

    //4.转换数据结构     ((ad,hm),count) ==> (ad,(hm,count))
    val adToHmCount: DStream[(String, (String, Int))] = adHmToCountDStream.map { case ((ad, hm), count) =>
      (ad, (hm, count))
    }

    //5.按照广告进行分组  (ad,(hm,count)) ==> (ad,Iter[(hm,count)...])
    val adToHmCountIter: DStream[(String, List[(String, Int)])] = adToHmCount.groupByKey().mapValues(iter => iter.toList.sortWith(_._1 < _._1))

    //6.打印
    adToHmCountIter.print()
  }

}
