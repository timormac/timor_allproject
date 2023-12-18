package k_Thread;

/**
 * @Title: A1_ThreadSimple
 * @Package: k_Thread
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 20:21
 * @Version:1.0
 */
public class A1_ThreadSimple {

    public static void main(String[] args) {


        //持续监听端口程序有问题,gpt给的代码监听不到数据
        Thread thread = new Thread("线程1");


        Thread thread2 = new Thread("线程2");
        thread.start();
        thread2.start();



    }


}
