package a4_cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @Title: a2_cache.A2_Cache
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/18 16:45
 * @Version:1.0
 */
public class A2_Cache {

    public static void main(String[] args) {

        // 创建缓存
        Cache<String, String> cache = CacheBuilder.newBuilder()
                .maximumSize(100) // 设置缓存的最大容量
                .expireAfterWrite(10, TimeUnit.MINUTES) // 设置写入后的过期时间
                .build();

        // 向缓存中存入数据
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");

        // 从缓存中获取数据
        String value1 = cache.getIfPresent("key1");
        System.out.println("Value for key1: " + value1);

        // 从缓存中获取不存在的数据
        String value4 = cache.getIfPresent("key4");
        System.out.println("Value for key4: " + value4); // 输出为null

        // 手动清除缓存中的数据
        cache.invalidate("key2");

        // 打印缓存中的所有数据
        System.out.println("Cache contents:");
        cache.asMap().forEach((key, value) -> System.out.println(key + ": " + value));

    }
}
