package com.example;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController  // 标识这是一个 REST 风格的 Controller
public class HelloController {
    // 处理根路径的 GET 请求
    @GetMapping("/")
    public String hello() {
        return "Hello, World!";
    }
} 