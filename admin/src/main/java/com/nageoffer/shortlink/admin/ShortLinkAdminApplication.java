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

package com.nageoffer.shortlink.admin;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * 短链接后管应用
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@SpringBootApplication
//开启服务注册与发现功能。
//如果不加这个注解（或者不开启这个功能），你的服务就是一个离线的单机应用，Nacos（服务中心）根本不知道它的存在，它也无法向 Nacos 询问别人的地址。
@EnableDiscoveryClient
//启用 OpenFeign 组件，扫描并创建远程调用接口的代理对象。
//这个注解告诉 Spring Boot：“去指定的包下面找带有 @FeignClient 的接口，帮我生成动态代理实现类，把它们变成真正的 HTTP 请求工具。”
@EnableFeignClients("com.nageoffer.shortlink.admin.remote")
@MapperScan("com.nageoffer.shortlink.admin.dao.mapper")
public class ShortLinkAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShortLinkAdminApplication.class, args);
    }
}
