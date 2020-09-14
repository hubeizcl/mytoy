package com.mytoy.starter.tools;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


@Slf4j
public class FilesUtils {

    public static byte[] readFile4ClassPath(String path) {
        Resource resource = new DefaultResourceLoader().getResource(path);
        if (resource.exists()) {
            try (InputStream inputStream = resource.getInputStream();
                 ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                byte[] buff = new byte[100];
                int rc;
                while ((rc = inputStream.read(buff, 0, 100)) > 0) byteArrayOutputStream.write(buff, 0, rc);
                return byteArrayOutputStream.toByteArray();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
        return null;
    }

    public static Properties getProperty4ClassPath(String path) {
        byte[] bytes = readFile4ClassPath(path);
        Properties properties = new Properties();
        if (null != bytes) {
            try {
                properties.load(new ByteArrayInputStream(bytes));
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
        return properties;
    }

    public static String getString4ClassPath(String path) {
        byte[] bytes = readFile4ClassPath(path);
        String str = "";
        if (null != bytes) str = new String(bytes);
        return str;
    }

    public static <T> T getBean4ClassPathJson(String path, Class<T> tClass) {
        byte[] bytes = readFile4ClassPath(path);
        T t = null;
        try {
            t = tClass.newInstance();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        if (null != bytes)
            t = JSON.parseObject(bytes, tClass);
        return t;
    }
}
