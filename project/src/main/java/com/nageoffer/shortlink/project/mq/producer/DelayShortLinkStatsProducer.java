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

package com.nageoffer.shortlink.project.mq.producer;

import cn.hutool.core.lang.UUID;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.DELAY_QUEUE_STATS_KEY;

/**
 * 延迟消费短链接统计发送者
 * <p>
 * 作用：将短链接统计消息投递到 Redisson 的延迟队列中。
 * 场景：通常由 ShortLinkStatsSaveConsumer 在获取分布式锁失败时调用。
 */
@Component
@RequiredArgsConstructor
public class DelayShortLinkStatsProducer {

    private final RedissonClient redissonClient;

    /**
     * 发送延迟消费短链接统计
     *
     * @param statsRecord 短链接统计实体参数（包含 IP、Browser、OS 等信息）
     */
    public void send(ShortLinkStatsRecordDTO statsRecord) {
        // 1. 生成本次延迟任务的唯一标识 (Keys)
        // 这一步非常关键！因为 ShortLinkStatsRecordDTO 本身可能是一个复用的数据结构。
        // 当它进入延迟队列时，被视为一个新的“任务”。
        // 这个 UUID 将被 MessageQueueIdempotentHandler 用来判断这个特定的延迟任务是否被消费过。
        statsRecord.setKeys(UUID.fastUUID().toString());

        // 2. 获取目标阻塞队列 (RBlockingDeque)
        // 这是消息最终“到期”后存放的地方，消费者会从这里 take() 或 poll() 消息。
        RBlockingDeque<ShortLinkStatsRecordDTO> blockingDeque = redissonClient.getBlockingDeque(DELAY_QUEUE_STATS_KEY);

        // 3. 获取延迟队列门面 (RDelayedQueue)
        // 这是一个特殊的包装器，它负责管理定时任务。它不会立刻把消息放进 blockingDeque，
        // 而是先放在一个内部的 ZSET 中，等时间到了，再自动移动到 blockingDeque。
        RDelayedQueue<ShortLinkStatsRecordDTO> delayedQueue = redissonClient.getDelayedQueue(blockingDeque);

        // 4. 投递消息，设置 5 秒延迟
        // 含义：这条消息现在发出去，但消费者在 5 秒之内是看不到它的。
        // 目的：给正在持有锁的线程一点时间去完成它的工作（比如分组修改）。5秒后再次重试，大概率锁已经释放了。
        delayedQueue.offer(statsRecord, 5, TimeUnit.SECONDS);
    }
}