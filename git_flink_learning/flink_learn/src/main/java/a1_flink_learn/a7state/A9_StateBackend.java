package a1_flink_learn.a7state;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;

/**
 * @Title: A9_StateBackend
 * @Package: com.timor.flink.learning.a7state
 * @Description:
 * @Author: XXX
 * @Date: 2023/7/28 16:16
 * @Version:1.0
 */
public class A9_StateBackend {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        /**
         * TODO 状态后端
         * 1、负责管理 本地状态
         * 2、 hashmap
         *          存在 TM的 JVM的堆内存，  读写快，缺点是存不了太多（受限与TaskManager的内存）
         *     rocksdb
         *          存在 TM所在节点的rocksdb数据库，存到磁盘中，  写--序列化，读--反序列化
         *          读写相对慢一些，可以存很大的状态
         *
         * 3、配置方式
         *    1）配置文件 默认值  flink-conf.yaml
         *    2）代码中指定
         *    3）提交参数指定
         *    flink run-application -t yarn-application
         *    -p 3
         *    -Dstate.backend.type=rocksdb
         *    -c 全类名
         *    jar包
         */

        // 1. 使用 hashmap状态后端
        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend);
        // 2. 使用 rocksdb状态后端
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        env.setStateBackend(embeddedRocksDBStateBackend);

        env.socketTextStream("localhost",7777);


    }
}
