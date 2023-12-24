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
        //finally的意义,和放在外面的语句还是不同的
        tryDemo();
    }

    public static  int tryDemo(){
        try {
            int result = 1;
            System.out.println(result);
            return 1;
        } catch (ArithmeticException e) {
            System.out.println(e);
        }finally {
            //即使有return也能执行
            System.out.println("即使return我也能执行");
        }
        //当try中有return操作,这里不能执行，而finally能执行
        System.out.println( "return了我不能执行");
        return 3;
    }

}
