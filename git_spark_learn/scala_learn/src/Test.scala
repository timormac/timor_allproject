

/**
 * @Title: Test
 * @Package:
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/4 17:40
 * @Version:1.0
 */
object Test {

  def main(args: Array[String]): Unit = {
    val  stu = new Student(11)
    println(stu.age)
    println(stu.sex)
    println(stu.id)
    val array = new Array(5)
  }

}


class Student(idPara:Int){

  var name:String ="alice"
  var age:Int = _
  var sex:String = _
  var id :Int = idPara



}
