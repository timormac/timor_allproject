package atguigu.streaming.day02

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Spark02_WordCount_CustomerReceiver {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark01_WordCount_RDD").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.创建RDD队列
    val queue = new mutable.Queue[RDD[Int]]()

    //4.从自定义数据源中加载数据创建流
    val lineDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))

    //5.计算WordCount并打印
    lineDStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //6.启动任务
    ssc.start()
    //7.阻塞
    ssc.awaitTermination()

  }

}

//自定义数据源
class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //任务启动时调用的方法
  override def onStart(): Unit = {
    new Thread("socket") {
      override def run(): Unit = {
        //负责接收&保存数据
        receive()
      }
    }.start()
  }

  def receive(): Unit = {

    //1.接收数据
    val socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

    var line: String = reader.readLine()

    while (isStarted() && line != null) {
      //2.保存数据
      store(line)
      line = reader.readLine()
    }

    reader.close()
    socket.close()
    restart("接收器重启")

  }


  //任务关闭时调用的方法
  override def onStop(): Unit = {}
}