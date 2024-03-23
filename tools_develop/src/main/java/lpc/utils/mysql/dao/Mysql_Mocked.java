package lpc.utils.mysql.dao;

/**
 * @Author Timor
 * @Date 2024/2/24 16:52
 * @Version 1.0
 */
public interface Mysql_Mocked {

    //模拟数据的生成方法
    void mock(String... strings) throws InterruptedException;
}
