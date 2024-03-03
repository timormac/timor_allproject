package a2_summary.b1_test;

/**
 * @Author Timor
 * @Date 2024/3/1 12:04
 * @Version 1.0
 */
public class Test2 {

    public static void main(String[] args) {

        String s = "2021-12-31 17:35:9.522,21540311,WET,5242,1xjb3iwn5bxdp9@3721.net,上海,139.7,\"{\"\"ismain\"\":0\",level:67,ts:48,ver:45}";
        String[] split = s.split(",");
        for (String s1 : split) {
            System.out.println(s1);
        }


    }
}
