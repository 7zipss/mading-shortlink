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

package com.nageoffer.shortlink.project.config;

import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Sentinel 限流规则初始化配置类
 * <p>
 * 作用：在应用启动阶段，加载定义的流控规则到 Sentinel 的内存管理器中。
 */
@Component
public class SentinelRuleConfig implements InitializingBean {

    /**
     * Spring Bean 初始化完成后自动调用的回调方法
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // 1. 创建一个存放流控规则的集合
        List<FlowRule> rules = new ArrayList<>();

        // 2. 定义具体的流控规则对象
        FlowRule createOrderRule = new FlowRule();

        // 3. 设置受保护的资源名称 (Resource Name)
        // 这里的 "create_short-link" 必须与 Controller 接口上的 @SentinelResource 注解值，
        // 或者 Sentinel 自动生成的资源名（如 URL 路径）保持一致。
        createOrderRule.setResource("create_short-link");

        // 4. 设置限流模式 (Grade)
        // RuleConstant.FLOW_GRADE_QPS：按照 QPS (每秒请求数) 进行限流
        // RuleConstant.FLOW_GRADE_THREAD：按照并发线程数进行限流
        createOrderRule.setGrade(RuleConstant.FLOW_GRADE_QPS);

        // 5. 设置限流阈值 (Count)
        // 这里设置为 1，意味着每秒最多只能允许 1 个请求通过，超过的会被拦截（Block）。
        createOrderRule.setCount(1);

        // 6. 将配置好的规则加入集合
        rules.add(createOrderRule);

        // 7. 加载规则到 Sentinel 的规则管理器中，使其生效
        FlowRuleManager.loadRules(rules);
    }
}