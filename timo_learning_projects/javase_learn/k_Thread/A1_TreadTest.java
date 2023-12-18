package k_Thread;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @Title: A1_TreadTest
 * @Package: k_Thread
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 19:47
 * @Version:1.0
 */

public class A1_TreadTest {

    public static void main(String[] args) {

        String data = null;
        Thread1 t1 = new Thread1("t1", data);
        Thread2 t2 = new Thread2("t2",data);
        t1.start();
        t2.start();


    }

}
class Thread1 extends Thread{
    String   data ="";
    public Thread1(String name,String data) {
        super(name);
        this.data = data;
    }
    @Override
    public void run() {
        int port = 8598;
        try {
            // 创建ServerSocket对象，监听指定端口
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("Listening on port " + port);
            while (true) {
                // 等待客户端连接
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getInetAddress().getHostAddress());
                // 获取输入流
                InputStream inputStream = clientSocket.getInputStream();
                // 读取数据
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    String s = new String(buffer, 0, bytesRead);
                    System.out.println(s);
                    String data = s;
                    sleep(1000L);

                }
                // 关闭连接
                clientSocket.close();
                System.out.println("Client disconnected");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        super.run();
    }
}

class Thread2 extends Thread {
    String data="";
    String log;

    public Thread2(String name, String data) {
        super(name);
        this.data = data;
        this.log = data;
    }

    @Override
    public void run() {
        super.run();
        try {
            while (true){
                if( data==log  ) { }
                else System.out.println();
                sleep(1000L);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}