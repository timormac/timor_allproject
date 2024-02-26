package a1_flink_learn;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;

/**
 * @Title: Test
 * @Package: com.timor.flink.learning
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/1 18:40
 * @Version:1.0
 */
public class Test {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDS = env.readTextFile("input_data/wordcount.txt");

        //flatMap方法需要传一个对象，是实现了FlatMapFunction接口的对象
        FlatMapOperator<String, Object> flatmap = lineDS.flatMap((value, out) -> {
            String[] splits = value.split(" ");
            for (String split : splits) out.collect(split);
        });


        env.execute();

    }

}

