package a4_implict

/**
 * @Title: A2_ClassDemo
 * @Package: a4_implict
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 23:37
 * @Version:1.0
 */
object A2_ClassDemo {

  //在Mysql类中，可以调用，不过要导包
  //DB1会对应生成隐式类
  implicit class DB1(val m: MySQL) {
    def addSuffix(): Unit = print(1)
  }



}
