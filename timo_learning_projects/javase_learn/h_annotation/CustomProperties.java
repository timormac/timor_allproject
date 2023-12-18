package h_annotation;

/**
 * @Title: CustomProperties
 * @Package: h_annotation
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 18:57
 * @Version:1.0
 */
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class CustomProperties {
    private Properties properties;

    public CustomProperties(String filePath) {
        properties = new Properties();
        try {
            FileInputStream fis = new FileInputStream(filePath);
            properties.load(fis);
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}