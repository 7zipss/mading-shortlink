/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.shortlink.admin.common.biz.user;

import com.alibaba.fastjson2.JSON;
import com.google.common.collect.Lists;
import com.nageoffer.shortlink.admin.common.convention.exception.ClientException;
import com.nageoffer.shortlink.admin.common.convention.result.Results;
import com.nageoffer.shortlink.admin.config.UserFlowRiskControlConfiguration;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Optional;

import static com.nageoffer.shortlink.admin.common.convention.errorcode.BaseErrorCode.FLOW_LIMIT_ERROR;

/**
 * 用户操作流量风控过滤器
 * <p>
 * 作用：拦截所有 HTTP 请求，检查当前用户在指定时间窗口内的访问次数是否超限。
 */
@Slf4j
@RequiredArgsConstructor
public class UserFlowRiskControlFilter implements Filter {

    // 注入 Redis 操作模板，用于执行 Lua 脚本
    private final StringRedisTemplate stringRedisTemplate;
    // 注入限流配置类（包含：是否开启、时间窗口大小、最大访问次数）
    private final UserFlowRiskControlConfiguration userFlowRiskControlConfiguration;

    // Lua 脚本的类路径地址
    private static final String USER_FLOW_RISK_CONTROL_LUA_SCRIPT_PATH = "lua/user_flow_risk_control.lua";

    /**
     * 过滤器的核心逻辑
     *
     * @param request  HTTP 请求
     * @param response HTTP 响应
     * @param filterChain 过滤器链
     */
    @SneakyThrows
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        // 1. 初始化 Redis Lua 脚本对象
        // 这里指定了脚本的返回值类型为 Long
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        // 加载 classpath 下的 lua 文件
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(USER_FLOW_RISK_CONTROL_LUA_SCRIPT_PATH)));
        redisScript.setResultType(Long.class);

        // 2. 获取当前请求的用户标识
        // UserContext.getUsername() 通常是从 ThreadLocal 中获取（由上游的 UserTransmitFilter 设置）
        // 如果获取不到（例如未登录接口），默认为 "other"，对所有未登录用户统一限流
        String username = Optional.ofNullable(UserContext.getUsername()).orElse("other");

        Long result = null;
        try {
            // 3. 执行 Redis Lua 脚本
            // execute 参数说明：
            // - redisScript: 脚本对象
            // - Lists.newArrayList(username): KEYS[1]，Redis 中的 Key 后缀
            // - userFlowRiskControlConfiguration.getTimeWindow(): ARGV[1]，时间窗口（秒）
            result = stringRedisTemplate.execute(redisScript, Lists.newArrayList(username), userFlowRiskControlConfiguration.getTimeWindow());
        } catch (Throwable ex) {
            // 4. 容错处理
            // 如果 Redis 执行报错（如 Redis 挂了或网络超时），这里记录错误日志
            log.error("执行用户请求流量限制LUA脚本出错", ex);
            // 【策略选择】这里选择了“宁杀错不放过”（Fail-Fast），直接返回限流错误。
            // 另一种策略是 catch 后放行（Fail-Safe），取决于业务对稳定性的要求。
            returnJson((HttpServletResponse) response, JSON.toJSONString(Results.failure(new ClientException(FLOW_LIMIT_ERROR))));
        }

        // 5. 检查结果是否超限
        // result 是 Lua 脚本返回的当前访问次数（已经 +1 后的值）
        // 如果 result 为空（执行异常）或者 result > 配置的最大次数
        if (result == null || result > userFlowRiskControlConfiguration.getMaxAccessCount()) {
            // 6. 拦截请求，返回错误信息
            // 构造统一的 JSON 返回体，告知前端“当前流量过大”
            returnJson((HttpServletResponse) response, JSON.toJSONString(Results.failure(new ClientException(FLOW_LIMIT_ERROR))));
        }

        // 7. 放行
        // 如果未超限，调用过滤器链的下一个节点（最终到达 Controller）
        filterChain.doFilter(request, response);
    }

    /**
     * 辅助方法：向响应流中写入 JSON 数据
     * * 因为 Filter 不像 Controller 那样支持自动对象序列化返回，
     * 所以需要手动设置 Content-Type 并写入 Response 流。
     */
    private void returnJson(HttpServletResponse response, String json) throws Exception {
        response.setCharacterEncoding("UTF-8");
        // 设置响应类型为 JSON
        response.setContentType("text/html; charset=utf-8"); // 注：这里虽然写了 text/html，但在 API 接口中通常建议用 application/json
        try (PrintWriter writer = response.getWriter()) {
            writer.print(json);
        }
    }
}