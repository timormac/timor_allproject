package a2_summary.a8_sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author Timor
 * @Date 2024/2/28 17:27
 * @Version 1.0
 */
public class A4_自定义函数 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE tb1 ( id String) WITH('connector' = 'datagen')" );

        // TODO udf函数
        //注册函数
        tableEnv.createTemporaryFunction("HashFunction", HashFunction.class);
        //sql写法
        Table table = tableEnv.sqlQuery("select HashFunction(id) from tb1");
        //table api用法,比较麻烦别用
        table.select(call("HashFunction",$("id"))).execute().print();


        // TODO 聚合函数
        tableEnv.createTemporaryFunction("WeightedAvg", WeightedAvg.class);
        tableEnv.sqlQuery("select name,WeightedAvg(score,weight)  from scores group by name");

        //TODO UDTF
        tableEnv.createTemporaryFunction("SplitFunction", SplitFunction.class);

        // 重命名侧向表中的字段
        tableEnv.sqlQuery("select words,newWord,newLength from str left join lateral table(SplitFunction(words))  as T(newWord,newLength) on true");
                // 3.1 交叉联结
//                .sqlQuery("select words,word,length from str,lateral table(SplitFunction(words))")
                // 3.2 带 on  true 条件的 左联结
//                .sqlQuery("select words,word,length from str left join lateral table(SplitFunction(words)) on true")




        //TODO 不知道干什么的table Agg函数,只能用 Table API,不能用sql
        tableEnv.createTemporaryFunction("Top2", Top2.class);
        table.flatAggregate(call("Top2", $("num")).as("value", "rank"))
                .select( $("value"), $("rank"))
                .execute().print();


    }

    // TODO 1.定义 自定义函数的实现类
    static class HashFunction extends ScalarFunction {

        // 接受任意类型的输入，返回 INT型输出
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.hashCode();
        }
    }


    // TODO 1.继承 AggregateFunction< 返回类型，累加器类型<加权总和，权重总和> >
    public static class WeightedAvg extends AggregateFunction<Double, Tuple2<Integer, Integer>> {

        @Override
        public Double getValue(Tuple2<Integer, Integer> integerIntegerTuple2) {
            return integerIntegerTuple2.f0 * 1D / integerIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        /**
         * 累加计算的方法，每来一行数据都会调用一次
         * @param acc 累加器类型
         * @param score 第一个参数：分数
         * @param weight 第二个参数：权重
         */
        public void accumulate(Tuple2<Integer, Integer> acc,Integer score,Integer weight){
            acc.f0 += score * weight;  // 加权总和 =  分数1 * 权重1 + 分数2 * 权重2 +....
            acc.f1 += weight;         // 权重和 = 权重1 + 权重2 +....
        }
    }

    // TODO 1.继承 TableFunction<返回的类型>
    // 类型标注： Row包含两个字段：word和length
    @FunctionHint(output = @DataTypeHint("ROW<word STRING,length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        // 返回是 void，用 collect方法输出
        public void eval(String str) {
            for (String word : str.split(" ")) {
                collect(Row.of(word, word.length()));
            }
        }
    }

    // TODO 1.继承 TableAggregateFunction< 返回类型，累加器类型<加权总和，权重总和> >
    // 返回类型 (数值，排名) =》 (12,1) (9,2)
    // 累加器类型 (第一大的数，第二大的数) ===》 （12,9）
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }


        /**
         * 每来一个数据调用一次，比较大小，更新 最大的前两个数到 acc中
         *
         * @param acc 累加器
         * @param num 过来的数据
         */
        public void accumulate(Tuple2<Integer, Integer> acc, Integer num) {
            if (num > acc.f0) {
                // 新来的变第一，原来的第一变第二
                acc.f1 = acc.f0;
                acc.f0 = num;
            } else if (num > acc.f1) {
                // 新来的变第二，原来的第二不要了
                acc.f1 = num;
            }
        }


        /**
         * 输出结果： （数值，排名）两条最大的
         *
         * @param acc 累加器
         * @param out 采集器<返回类型>
         */
        public void emitValue(Tuple2<Integer, Integer> acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.f0 != 0) {
                out.collect(Tuple2.of(acc.f0, 1));
            }
            if (acc.f1 != 0) {
                out.collect(Tuple2.of(acc.f1, 2));
            }
        }
    }



}




