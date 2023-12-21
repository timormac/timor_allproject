package guava.a2_string_utils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.List;

/**
 * @Title: A1_String
 * @Package: a2_string_utils
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/18 17:04
 * @Version:1.0
 */
public class A1_String {

    public static void main(String[] args) {
        //字符串判空
        String str = "   hello,world   ";
        boolean isNullOrEmpty = Strings.isNullOrEmpty(str);

        //获取字符串长度
        int length = Strings.nullToEmpty(str).length();

        //去除字符串前后空格
        String trimmed = Strings.nullToEmpty(str).trim();

        //字符串填充
        String padded = Strings.padEnd(str, 10, '!');

        //字符串连接
        List<String> list = Arrays.asList("hello", "world");
        String joined = Joiner.on(", ").join(list);

        //字符串拆分
        List<String> parts = Splitter.on(",").trimResults().splitToList(str);

    }
}
