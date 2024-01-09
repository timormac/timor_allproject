package atguigu.streaming.practice.handler

import java.sql.Connection

import com.atguigu.practice.bean.Ads_log
import com.atguigu.practice.utils.JdbcUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream

object BlackListHandler {

  /**
    * 根据黑名单过滤数据
    *
    * @param adsLogDStream 原始数据
    * @return 过滤后的数据
    */
  def filterByBlackList(adsLogDStream: DStream[Ads_log], sc: SparkContext): DStream[Ads_log] = {

    //使用mapPartitions代替filter,减少连接的创建与释放
    //    val value: DStream[Ads_log] = adsLogDStream.mapPartitions(iter => {
    //
    //      //a.获取连接
    //      val connection: Connection = JdbcUtil.getConnection
    //
    //      //b.操作数据
    //      val filterIter: Iterator[Ads_log] = iter.filter(log => {
    //        val bool: Boolean = JdbcUtil.isExist(connection,
    //          "select userid from black_list where userid=?;",
    //          Array(log.userid))
    //        println(bool)
    //        !bool
    //      })
    //
    //      //c.关闭连接
    //      connection.close()
    //
    //      //d.返回数据
    //      filterIter
    //    })

    //使用广播变量,每个批次获取一次黑名单数据,发送至Executor端
    val value1: DStream[Ads_log] = adsLogDStream.transform(rdd => {

      //获取连接
      val connection: Connection = JdbcUtil.getConnection
      //查询黑名单数据
      val userList: List[String] = JdbcUtil.getBlackListFromMysql(connection, "select userid from black_list;", Array())
      //将黑名单数据广播出去
      val userListBC: Broadcast[List[String]] = sc.broadcast(userList)
      //关闭连接
      connection.close()

      rdd.filter(log => !userListBC.value.contains(log.userid))
    })

    value1
  }


  /**
    * 将当前批次的数据结合MySQL中已有的数据写出
    * 然后读取用户对于某个广告的单日点击总次数
    * 如果点击总次数超过100,则将该用户写入黑名单(Mysql)
    *
    * @param dateUserAdToCountDStream 经过黑名单过滤后的数据集计算的广告点击次数
    */
  def saveBlackListToMysql(dateUserAdToCountDStream: DStream[((String, String, String), Int)]): Unit = {

    dateUserAdToCountDStream.foreachRDD(rdd => {

      //分区操作,减少连接的创建和释放
      rdd.foreachPartition(iter => {

        //a.获取连接
        val connection: Connection = JdbcUtil.getConnection

        //b.遍历数据操作
        iter.foreach { case ((date, user, ad), ct) =>

          //b.1 将当前批次的数据结合MySQL中已有的数据写出
          JdbcUtil.executeUpdate(connection,
            """
              |INSERT INTO user_ad_count(dt,userid,adid,count)
              |VALUES(?,?,?,?)
              |ON DUPLICATE KEY
              |UPDATE count=count+?;
            """.stripMargin,
            Array(date, user, ad, ct, ct))

          //b.2 然后读取用户对于某个广告的单日点击总次数
          val userAdCount: Long = JdbcUtil.getDataFromMysql(connection,
            """
              |select
              |    count
              |from
              |    user_ad_count
              |where
              |    dt=? and userid=? and adid=?;
            """.stripMargin,
            Array(date, user, ad))

          //b.3 如果点击总次数超过100,则将该用户写入黑名单(Mysql)
          if (userAdCount >= 500L) {
            JdbcUtil.executeUpdate(connection,
              """
                |INSERT INTO black_list(userid)
                |VALUES(?)
                |ON DUPLICATE KEY
                |UPDATE userid=?;
                |""".stripMargin,
              Array(user, user))
          }
        }

        //c.关闭连接
        connection.close()

      })

    })

  }

}
