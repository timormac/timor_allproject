package atguigu.sparksql.scala01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaWordCount {

    public static void main(String[] args) {

        //1.创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");

        //2.创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //3.读取文件创建JavaRDD
        JavaRDD<String> lineRDD = jsc.textFile("./input/1.txt");

        //4.扁平化 line==>word
        JavaRDD<String> wordRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split).iterator();
            }
        });

        //5.将单词映射为元组 word==>(word,1)
        JavaPairRDD<String, Integer> wordToOneRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2(s, 1);
            }
        });

        //6.计算单词出现的总数 (word,1)==>(word,count)
        JavaPairRDD<String, Integer> wordToCountRDD = wordToOneRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //7.打印
        for (Tuple2<String, Integer> tuple2 : wordToCountRDD.collect()) {
            System.out.println(tuple2);
        }

        //8.关闭资源
        jsc.stop();

    }

}
