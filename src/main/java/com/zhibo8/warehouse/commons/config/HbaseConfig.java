package com.zhibo8.warehouse.commons.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "application.properties")
@ConfigurationProperties(prefix = "hbase")
public class HbaseConfig {

}
