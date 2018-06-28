package com.zhibo8.warehouse.commons.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Properties;

@Component
public class SystemConfig {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static Properties props;

    public SystemConfig() {
        try {
            Resource resource = new ClassPathResource("/application.properties");//
            props = PropertiesLoaderUtils.loadProperties(resource);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }


    /**
     * 获取属性
     *
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return props == null ? null : props.getProperty(key);
    }

    /**
     * 获取属性
     *
     * @param key          属性key
     * @param defaultValue 属性value
     * @return
     */
    public static String getProperty(String key, String defaultValue) {
        return props == null ? null : props.getProperty(key, defaultValue);
    }

    /**
     * 获取properyies属性
     *
     * @return
     */
    public static Properties getProperties() {
        return props;
    }
}