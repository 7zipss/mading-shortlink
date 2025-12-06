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

package com.nageoffer.shortlink.project.initialize;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_GROUP_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_TOPIC_KEY;

/**
 * 初始化短链接监控消息队列消费者组
 * <p>
 * 作用：在应用启动时检查 Redis Stream 是否存在。如果不存，则创建 Stream 和消费者组。
 * 这防止了消费者在启动时因为找不到 Group 而报错。
 *
 * 1. 消费者组（consumer group）允许用户将一个流从逻辑上分成多个不同的流，并让消费者组组下的消费者去处理组中的消息
 * 2. 多个消费者组可以共享同一个流中的元素；但同一个消费者组中的每条消息只能有一个消费者，即不同的消费者将独占组中不同的消息，当一个消费者读取了组中的一条消息后，其他消费者将无法读取这条消息
 * 3. 就目前使用来看，一个流拥有一个消费者组是正常的，至于什么情况下需要使用多个消费者组还没发现，敬请指教
 *
 */
@Component
@RequiredArgsConstructor
public class ShortLinkStatsStreamInitializeTask implements InitializingBean {

    private final StringRedisTemplate stringRedisTemplate;

    /**
     * Spring Bean 属性注入完成后自动调用的方法
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // 1. 检查 Redis 中是否存在这个 Stream 的 Key
        // Key 常量：short-link:stats-stream
        Boolean hasKey = stringRedisTemplate.hasKey(SHORT_LINK_STATS_STREAM_TOPIC_KEY);

        // 2. 如果 Key 不存在（说明是第一次部署，或者 Redis 数据被清空了）
        if (hasKey == null || !hasKey) {
            // 3. 创建消费者组
            // opsForStream().createGroup() 方法对应 Redis 命令：XGROUP CREATE key groupname id [MKSTREAM]
            // 参数 1: Stream 的 Key
            // 参数 2: 消费者组名称 (例如: stats-consumer-group)
            // 隐含逻辑：Spring Data Redis 的这个方法通常会自动带上 MKSTREAM 选项，
            // 意味着如果 Stream 不存在，会自动创建一个空的 Stream，并建立组。
            stringRedisTemplate.opsForStream().createGroup(SHORT_LINK_STATS_STREAM_TOPIC_KEY, SHORT_LINK_STATS_STREAM_GROUP_KEY);
        }
    }
}