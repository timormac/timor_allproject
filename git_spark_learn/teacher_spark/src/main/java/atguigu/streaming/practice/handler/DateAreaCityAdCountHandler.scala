package atguigu.streaming.practice.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.practice.bean.Ads_log
import com.atguigu.practice.utils.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

object DateAreaCityAdCountHandler {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //计算每天各地区各城市各广告的点击总流量，并将其存入MySQL
  def saveResultToMysql(filterAdsLogDStream: DStream[Ads_log]): Unit = {

    //1.转换数据结构  Ads_log  ==>  ((date,area,city,ad),1)
    val dateAreaCityAdToOneDStream: DStream[((String, String, String, String), Int)] = filterAdsLogDStream.map(log => {

      //将时间戳转换为日期
      val timestamp: Long = log.timestamp
      val date: String = sdf.format(new Date(timestamp))

      ((date, log.area, log.city, log.adid), 1)
    })

    //2.计算总点击次数  ((date,area,city,ad),1) ==> ((date,area,city,ad),count)
    val dateAreaCityAdToCountDStream: DStream[((String, String, String, String), Int)] = dateAreaCityAdToOneDStream.reduceByKey(_ + _)

    //3.将数据保存至MySQL
    dateAreaCityAdToCountDStream.foreachRDD(rdd => {

      //分区操作
      rdd.foreachPartition(iter => {

        //a.获取连接
        val connection: Connection = JdbcUtil.getConnection

        //b.遍历操作数据
        iter.foreach { case ((date, area, city, ad), ct) =>
          JdbcUtil.executeUpdate(connection,
            """
              |INSERT INTO area_city_ad_count(dt,area,city,adid,count)
              |VALUES(?,?,?,?,?)
              |ON DUPLICATE KEY
              |UPDATE count=count+?;
            """.stripMargin,
            Array(date, area, city, ad, ct, ct))
        }

        //c.释放连接
        connection.close()

      })
    })

  }

}
