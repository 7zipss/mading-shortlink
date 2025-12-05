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

package com.nageoffer.shortlink.admin.config;

import com.nageoffer.shortlink.admin.common.biz.user.UserFlowRiskControlFilter;
import com.nageoffer.shortlink.admin.common.biz.user.UserTransmitFilter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 用户配置自动装配
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@Configuration
public class UserConfiguration {

    /**
     * 用户信息传递过滤器
     */
    @Bean
    public FilterRegistrationBean<UserTransmitFilter> globalUserTransmitFilter(StringRedisTemplate stringRedisTemplate) {
        FilterRegistrationBean<UserTransmitFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new UserTransmitFilter(stringRedisTemplate));
        registration.addUrlPatterns("/*");
        registration.setOrder(0);
        return registration;
    }

    /**
     * 注册“用户操作流量风控过滤器”
     * <p>
     * 作用：
     * 1. 实例化 UserFlowRiskControlFilter。
     * 2. 定义该过滤器的拦截范围和执行顺序。
     * 3. 通过 @ConditionalOnProperty 实现功能开关。
     */
    @Bean // 声明这是一个 Spring Bean，由容器管理
    // 【关键注解】条件装配
    // 含义：只有当配置文件(application.yaml)中 short-link.flow-limit.enable 属性的值为 true 时，
    // 这个 Bean 才会被创建。如果配置为 false 或未配置，则整个限流功能直接关闭，不占用资源。
    @ConditionalOnProperty(name = "short-link.flow-limit.enable", havingValue = "true")
    public FilterRegistrationBean<UserFlowRiskControlFilter> globalUserFlowRiskControlFilter(
            // 自动注入 Redis 模板，用于执行 Lua 脚本
            StringRedisTemplate stringRedisTemplate,
            // 自动注入限流配置类，获取窗口时间和最大访问次数
            UserFlowRiskControlConfiguration userFlowRiskControlConfiguration) {

        // 创建 Filter 注册对象，这是 Spring Boot 注册 Servlet Filter 的标准方式
        FilterRegistrationBean<UserFlowRiskControlFilter> registration = new FilterRegistrationBean<>();

        // 设置实际的过滤器实例
        // 这里手动 new 了一个 UserFlowRiskControlFilter，并将依赖组件传进去
        registration.setFilter(new UserFlowRiskControlFilter(stringRedisTemplate, userFlowRiskControlConfiguration));

        // 设置拦截规则
        // "/*" 表示拦截所有 HTTP 请求，这意味着任何接口访问都会经过风控检查
        registration.addUrlPatterns("/*");

        // 设置过滤器执行顺序
        // 值越小优先级越高。
        // 设置为 10 是为了确保它在 UserTransmitFilter (Order=0) 之后执行。
        // 因为风控逻辑需要先获取用户信息(UserContext)，而用户信息是在 UserTransmitFilter 中解析的。
        registration.setOrder(10);

        return registration;
    }
}