package a_varibletype;

/**
 * @Title: A3_TryCatch
 * @Package: a_varibletype
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/20 09:30
 * @Version:1.0
 */
public class A3_TryCatch {

    public static void main(String[] args) {

        try {
            // 可能会抛出异常的代码
            int result = 1/0;
            System.out.println(result);
        } catch (ArithmeticException e) {
            // 捕获并处理异常
            System.out.println("除数不能为零！");
        }


    }
}
