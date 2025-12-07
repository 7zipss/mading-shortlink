/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.shortlink.aggregation;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * 短链接聚合应用
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@EnableDiscoveryClient
//这是最关键的代码。
// Spring Boot 默认只扫描启动类所在包及其子包。
// 由于 admin 和 project 的包名（虽然都是 com.nageoffer.shortlink 但在不同模块）
// 不在 aggregation 包下，必须手动指定扫描路径。
@SpringBootApplication(scanBasePackages = {
        "com.nageoffer.shortlink.admin",   // [关键] 强制扫描 Admin 模块的 Bean (Controller, Service等)
        "com.nageoffer.shortlink.project"  // [关键] 强制扫描 Project 模块的 Bean
})
//TODO  ↑↓ 两个模块中出现了“同名类”时（比如都有回收站业务），需要通过value=xxxByAdmin避免同名Bean
@MapperScan(value = {
        "com.nageoffer.shortlink.project.dao.mapper", // [关键] 扫描 Project 的 MyBatis Mapper
        "com.nageoffer.shortlink.admin.dao.mapper"    // [关键] 扫描 Admin 的 MyBatis Mapper
})
public class AggregationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AggregationServiceApplication.class, args);
    }
}