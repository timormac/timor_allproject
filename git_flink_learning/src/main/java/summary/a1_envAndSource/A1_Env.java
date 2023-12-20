package summary.a1_envAndSource;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
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

        //TODO  env可设置参数

        //禁止算子
        en1.disableOperatorChaining();





    }

}
