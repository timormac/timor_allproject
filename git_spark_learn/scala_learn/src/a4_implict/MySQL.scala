package a4_implict

import a4_implict.A2_ClassDemo.DB1

/**
 * @Title: MySQL
 * @Package: a4_implict
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 23:38
 * @Version:1.0
 */
class MySQL {}
object MySQL{

  def main(args: Array[String]): Unit = {
    val mysql1 = new MySQL
    //可以调用DB1的全部方法
    mysql1.addSuffix()
  }

}
