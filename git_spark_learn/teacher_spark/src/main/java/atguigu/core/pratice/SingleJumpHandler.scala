package atguigu.core.pratice

import org.apache.spark.rdd.RDD

object SingleJumpHandler {

  //计算单跳访问次数:分子  (1_2,2_3,3_4...6_7)
  def getSingleJumpCount(lineRDD: RDD[String], fromToPages: Array[String]) = {

    //1.转换数据结构,将session作为Key,   line=>(session,(page,dt))
    val sessionToPageDt: RDD[(String, (String, String))] = lineRDD.map(line => {
      val arr: Array[String] = line.split("_")
      (arr(2), (arr(3), arr(4)))
    })

    //2.按照Session分组  (session,(page,dt)) ==> (session,Iter[(page,dt)...])
    val sessionToPageDtIter: RDD[(String, Iterable[(String, String)])] = sessionToPageDt.groupByKey()

    //3.组内按照时间排序,取出访问页面,组合成单跳数据并过滤
    // (map,flatMap) (mapValues,flatMap) (flatMap) (flatMapValues,map)
    //a.看是否需要Key
    //b.是否需要压平操作
    //需要Key且需要压平     ==> flatMapValues
    //需要Key但不需要压平   ==> mapValues
    //不需要Key但需要压平   ==> flatMap
    //不需要Key也不需要压平 ==> map
    val fromToPagesListRDD: RDD[List[String]] = sessionToPageDtIter.map { case (_, iter) =>
      //当前Session中访问过的页面
      val pages: List[String] = iter.toList.sortBy(_._2).map(_._1)
      //构建访问单跳
      val fromPages: List[String] = pages.dropRight(1)
      val toPages: List[String] = pages.drop(1)
      val sessionFromToPages: List[String] = fromPages.zip(toPages).map { case (from, to) =>
        s"${from}_$to"
      }
      //根据要求过滤数据
      sessionFromToPages.filter(x => fromToPages.contains(x))
    }
    //4.压平操作
    val fromToPagesToOne: RDD[(String, Int)] = fromToPagesListRDD.flatMap(x => x.map((_, 1)))
    //5.计算单跳总数
    fromToPagesToOne.reduceByKey(_ + _)

    //使用mapValues
    //    val sessionToFromToPagesListRDD: RDD[(String, List[String])] = sessionToPageDtIter.mapValues(iter => {
    //      //当前Session中访问过的页面
    //      val pages: List[String] = iter.toList.sortBy(_._2).map(_._1)
    //      //构建访问单跳
    //      val fromPages: List[String] = pages.dropRight(1)
    //      val toPages: List[String] = pages.drop(1)
    //      val sessionFromToPages: List[String] = fromPages.zip(toPages).map { case (from, to) =>
    //        s"${from}_$to"
    //      }
    //      //根据要求过滤数据
    //      sessionFromToPages.filter(x => fromToPages.contains(x))
    //    })
    //    sessionToFromToPagesListRDD.flatMap { case (_, list) =>
    //      list.map(x => (x, 1))
    //    }.reduceByKey(_ + _)
    //
    //    //使用FlatMap
    //    sessionToPageDtIter.flatMap { case (_, iter) =>
    //      //当前Session中访问过的页面
    //      val pages: List[String] = iter.toList.sortBy(_._2).map(_._1)
    //      //构建访问单跳
    //      val fromPages: List[String] = pages.dropRight(1)
    //      val toPages: List[String] = pages.drop(1)
    //      val sessionFromToPages: List[String] = fromPages.zip(toPages).map { case (from, to) =>
    //        s"${from}_$to"
    //      }
    //      //根据要求过滤数据
    //      sessionFromToPages.filter(x => fromToPages.contains(x))
    //        .map(x => (x, 1))
    //    }.reduceByKey(_ + _)


  }

  //计算单页访问次数:分母
  def getSinglePageCount(lineRDD: RDD[String], fromPages: Array[Int]): RDD[(String, Int)] = {

    //1.过滤
    val filterRDD: RDD[String] = lineRDD.filter(line => {
      val arr: Array[String] = line.split("_")
      fromPages.contains(arr(3).toInt)
    })

    //2.转换数据结构 line ==> (pageId,1)
    val pageIdToOne: RDD[(String, Int)] = filterRDD.map(line => {
      val arr: Array[String] = line.split("_")
      (arr(3), 1)
    })

    //3.计算当前页面访问总数
    pageIdToOne.reduceByKey(_ + _)

  }

}
