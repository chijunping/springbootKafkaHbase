package com.zhibo8.warehouse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

//import org.springframework.boot.web.support.SpringBootServletInitializer;
@EnableScheduling
@SpringBootApplication
public class WarehouseApplication{

    public static void main(String[] args) {
        SpringApplication.run(WarehouseApplication.class, args);
    }

}
//@SpringBootApplication
//@ServletComponentScan//打war 包时
//public class WarehouseApplication extends SpringBootServletInitializer {
//    @Override
//    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
//        return application.sources(WarehouseApplication.class);
//    }
//
//    public static void main(String[] args) {
//        SpringApplication.run(WarehouseApplication.class, args);
//    }
//
//}
