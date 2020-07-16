package com.atguigu.gmall0213.gmall0213logger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.atguigu.gmall0213.gmall0213logger.controller")
public class Gmall0213LoggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0213LoggerApplication.class, args);
    }

}
