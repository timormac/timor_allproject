package atguigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Transform_K_V_Practice {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取文件创建RDD   textFile
    val lineRDD: RDD[String] = sc.textFile("./input/agent.log")

    //4.转换数据结构,提取省份ID和广告ID  line==>(proId,adId)                  map
    //    val proToAd: RDD[(String, String)] = lineRDD.map(line => {
    //      val arr: Array[String] = line.split(" ")
    //      (arr(1), arr(4))
    //    })
    //    //5.标记每个省份每个广告点击一次     (proId,adId)==>((proId,adId),1)      map
    //    val value: RDD[((String, String), Int)] = proToAd.map(x => (x, 1))
    val proAndAdToOne: RDD[((String, String), Int)] = lineRDD.map(line => {
      val arr: Array[String] = line.split(" ")
      ((arr(1), arr(4)), 1)
    })

    //6.计算每个省份每个广告点击总数     ((proId,adId),1)==>((proId,adId),count) reduceByKey
    val proAndAdToCount: RDD[((String, String), Int)] = proAndAdToOne.reduceByKey(_ + _)

    //7.转换数据结构,将省份作为Key      ((proId,adId),count)==>(proId,(adId,count)) map
    val proToAdAndCount: RDD[(String, (String, Int))] = proAndAdToCount.map { case ((pro, ad), count) =>
      (pro, (ad, count))
    }

    //8.按照省份分组,将当前省份所有广告点击放入一个集合
    // (proId,(adId,count))  ==>  (proId,Iter[(adId,count)...])   groupByKey
    val proToAdAndCountIter: RDD[(String, Iterable[(String, Int)])] = proToAdAndCount.groupByKey()

    //9.提取广告点击Top3   mapValues
    val proToAdAndCountTop3: RDD[(String, List[(String, Int)])] = proToAdAndCountIter.mapValues(iter => {
      iter.toList.sortWith(_._2 > _._2).take(3)
    })

    //10.打印
    proToAdAndCountTop3.sortByKey().collect().foreach(println)

    //    (0,List((2,29), (24,25), (26,24)))
    //    (1,List((3,25), (6,23), (5,22)))
    //    (2,List((6,24), (21,23), (29,20)))
    //    (3,List((14,28), (28,27), (22,25)))
    //    (4,List((12,25), (2,22), (16,22)))
    //    (5,List((14,26), (21,21), (12,21)))
    //    (6,List((16,23), (24,21), (22,20)))
    //    (7,List((16,26), (26,25), (1,23)))
    //    (8,List((2,27), (20,23), (11,22)))
    //    (9,List((1,31), (28,21), (0,20)))

    //关闭连接
    sc.stop()

  }

}
