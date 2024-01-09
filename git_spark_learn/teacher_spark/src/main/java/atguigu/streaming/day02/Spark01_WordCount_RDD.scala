package atguigu.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Spark01_WordCount_RDD {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount_RDD").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.创建RDD队列
    val queue = new mutable.Queue[RDD[Int]]()

    //4.加载RDD队列中的数据创建流
    val rddDStream: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = false)

    //5.累加求和
    val sumDStream: DStream[Int] = rddDStream.reduce(_ + _)

    //6.打印
    sumDStream.print()

    //7.启动任务
    ssc.start()

    //8.向RDD队列中添加数据
    for (i <- 1 to 5) {
      queue += ssc.sparkContext.makeRDD(1 to 100, 10)
      Thread.sleep(2000)
    }

    //9.阻塞
    ssc.awaitTermination()


  }

}
