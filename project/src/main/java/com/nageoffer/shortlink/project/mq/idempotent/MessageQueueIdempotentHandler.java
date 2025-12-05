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

package com.nageoffer.shortlink.project.mq.idempotent;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 消息队列幂等处理器
 * <p>
 * 作用：利用 Redis 原子性防止消息重复消费，并管理消费状态（处理中/已完成）。
 * 原理：类似于分布式锁，Key 是消息 ID，Value 是处理状态。
 */
@Component
@RequiredArgsConstructor
public class MessageQueueIdempotentHandler {

    private final StringRedisTemplate stringRedisTemplate;

    // short-link:stats-stream中未处理的消息：不在short-link:idempotent:+messageId
    // short-link:stats-stream中正在处理的消息：在short-link:idempotent:+messageId中value=0
    // short-link:stats-stream中处理完成的消息：在short-link:idempotent:+messageId中value=1
    // short-link:stats-stream中处理失败的消息：将short-link:idempotent:+messageId删除

    // Redis Key 前缀，用于区分业务
    private static final String IDEMPOTENT_KEY_PREFIX = "short-link:idempotent:";

    /**
     * 【核心方法】判断当前消息是否可以被消费（尝试获取处理权）
     * * 逻辑：
     * 利用 Redis 的 SETNX (setIfAbsent) 命令。
     * 如果 Key 不存在，则设置 Key 值为 "0"（代表处理中），并返回 true —— 表示抢占成功，可以消费。
     * 如果 Key 已存在，则不进行操作，并返回 false —— 表示已被其他线程抢占或已处理，不可重复消费。
     *
     * @param messageId 消息唯一标识 (Message ID)
     * @return true: 抢占成功，允许消费; false: 抢占失败，禁止消费
     */
    public boolean isMessageProcessed(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        // setIfAbsent = SETNX (Set if Not eXists)
        // Value "0" 表示：正在处理中
        // 过期时间 2分钟：防止消费者宕机导致 Key 永久存在（死锁），2分钟后自动释放
        return Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(key, "0", 2, TimeUnit.MINUTES));
    }

    /**
     * 判断消息流程是否早已执行完成
     *
     * 场景：
     * 当 isMessageProcessed 返回 false 时，有两种情况：
     * 1. 别的线程正在处理（Value="0"）。
     * 2. 之前已经处理完了（Value="1"）。
     * 此方法用于区分这两种情况。如果返回 true，说明是重复消息且已完成，直接 ACK 即可。
     *
     * @param messageId 消息唯一标识
     * @return true: 已完成; false: 未完成（可能是不存在或正在处理）
     */
    public boolean isAccomplish(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        // 获取 Redis 中的状态值，判断是否为 "1"
        return Objects.equals(stringRedisTemplate.opsForValue().get(key), "1");
    }

    /**
     * 设置消息流程执行完成
     *
     * 场景：
     * 当消费者成功将数据写入数据库后，调用此方法将 Redis 状态修改为 "1"。
     *
     * @param messageId 消息唯一标识
     */
    public void setAccomplish(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        // 覆盖 Key 的值为 "1"（代表处理完成）
        // 再次设置过期时间 2分钟，保证幂等性窗口期
        stringRedisTemplate.opsForValue().set(key, "1", 2, TimeUnit.MINUTES);
    }

    /**
     * 删除幂等标识（回滚）
     *
     * 场景：
     * 如果在消费过程中抛出异常（如数据库挂了），需要让这条消息在重试时能再次被处理。
     * 所以要删除 Redis Key，让下一次 isMessageProcessed 能返回 true。
     *
     * @param messageId 消息唯一标识
     */
    public void delMessageProcessed(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        stringRedisTemplate.delete(key);
    }
}