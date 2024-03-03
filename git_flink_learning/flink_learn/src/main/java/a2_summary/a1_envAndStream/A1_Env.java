package a2_summary.a1_envAndStream;

import com.facebook.fb303.FacebookService;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title: A1_Env
 * @Package: summary.a1_envAndSource
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/20 14:30
 * @Version:1.0
 */
public class A1_Env {

    public static void main(String[] args) {

        //批处理环境,没有stream
        ExecutionEnvironment batch = ExecutionEnvironment.getExecutionEnvironment();

        //流处理环境
        StreamExecutionEnvironment en1 = StreamExecutionEnvironment.getExecutionEnvironment();
        //流处理本地运行带UI,登陆localhost:8081能看到flinkweb界面，pom文件还要导入flink-runtime-web依赖
        StreamExecutionEnvironment en2 = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //TODO  -------------------------------------env可设置参数--------------------------------------------

        //禁止算子链化, 算子链化能避免上下2个算子之间的序列化和反序列,最好开启//一般是用来找背压问题时启用
        en1.disableOperatorChaining();

        //设置并行度
        en1.setParallelism(1);

        //设置环境模式:是流还是批,默认是自动。官方建议提交flink任务时,指定参数来选择，而不是代码里
        en1.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //启用checkpoint，5分钟触发一次,模式是精准一次，还可以选择AT_LEAST_ONCE
        en1.enableCheckpointing(5*60*1000L, CheckpointingMode.EXACTLY_ONCE);

        // 代码中用到hdfs，需要导入hadoop依赖、指定访问hdfs的用户名
        System.setProperty("HADOOP_USER_NAME", "lpc");

        //checkpoint使用存在哪里
        en1.getCheckpointConfig().setCheckpointStorage("hdfs://project1:8020/flink_cep");

        //设置changelog状态后端,这样checkpoint就可以记录增量变化,保存时更快。pom导入flink-statebackend-rocksdb依赖
        // 要求checkpoint的最大并发必须为1，其他参数建议在flink-conf配置文件中去指定
        en1.enableChangelogStateBackend(true);


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


        //源码注释:checkpoints based on the configured {@link org.apache.flink.runtime.state.CheckpointStorage},当你开启ckp会自动存到指定位置
        //设置状态后端,当声明带状态的变量valueState时,默认是memoryStateBackend,我们要手动去指定HashMapStateBackend
        en1.setStateBackend(new HashMapStateBackend());

        //源码注释please configure a {@link* org.apache.flink.runtime.state.CheckpointStorage}
        //设置状态后端为rocksdb,同时,如果启用增量检查点就输入true,不需要就不带参数
        en1.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        //状态后端存在后端文件系统
        en1.setStateBackend( new FsStateBackend("hdfs://project1:8020/flink_cep"));

        //TODO  -------------------------------------env可获取的--------------------------------------------

        //获取checkpoint配置,具体详情配置看checkpoint章节
        CheckpointConfig checkpointConfig = en1.getCheckpointConfig();


    }

}
