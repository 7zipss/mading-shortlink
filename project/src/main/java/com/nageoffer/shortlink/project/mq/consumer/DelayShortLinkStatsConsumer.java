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

package com.nageoffer.shortlink.project.mq.consumer;

import com.nageoffer.shortlink.project.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.nageoffer.shortlink.project.mq.idempotent.MessageQueueIdempotentHandler;
import com.nageoffer.shortlink.project.service.ShortLinkService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.DELAY_QUEUE_STATS_KEY;

/**
 * 延迟记录短链接统计组件
 * <p>
 * 作用：监听延迟队列，处理那些因并发锁竞争而暂时无法入库的统计消息。
 * 策略：取出到期消息 -> 重新投递回 Redis Stream 主队列 -> 让主消费者再次尝试入库。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DelayShortLinkStatsConsumer implements InitializingBean {

    private final RedissonClient redissonClient;
    private final ShortLinkService shortLinkService;
    private final MessageQueueIdempotentHandler messageQueueIdempotentHandler;

    /**
     * 启动消费者线程逻辑
     */
    public void onMessage() {
        // 创建一个单线程池，专门用于运行这个无限循环的监听任务
        Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable);
                            // 命名线程，方便排查问题（如 jstack 查看堆栈）
                            thread.setName("delay_short-link_stats_consumer");
                            // 设置为守护线程，确保应用关闭时该线程也会自动退出，不会阻止 JVM 关闭
                            thread.setDaemon(Boolean.TRUE);
                            return thread;
                        })
                .execute(() -> {
                    // 1. 获取基础阻塞队列 (RBlockingDeque)
                    // 这是延迟消息到期后最终存放的“容器”
                    RBlockingDeque<ShortLinkStatsRecordDTO> blockingDeque = redissonClient.getBlockingDeque(DELAY_QUEUE_STATS_KEY);

                    // 2. 获取延迟队列接口 (RDelayedQueue)
                    // 这是 Redisson 提供的延迟功能封装，负责将到期消息从 ZSET 移动到上面的 blockingDeque
                    RDelayedQueue<ShortLinkStatsRecordDTO> delayedQueue = redissonClient.getDelayedQueue(blockingDeque);

                    // 3. 开启死循环，持续轮询
                    for (; ; ) {
                        try {
                            // 4. 尝试拉取消息
                            // poll(): 获取并移除队列头部的元素，如果队列为空则立即返回 null
                            ShortLinkStatsRecordDTO statsRecord = delayedQueue.poll();

                            if (statsRecord != null) {
                                // 5. 幂等性检查
                                // statsRecord.getKeys() 是在 Producer 中生成的 UUID
                                // 检查这个特定的“延迟任务”是否已经被处理过（防止重复重试）
                                if (!messageQueueIdempotentHandler.isMessageProcessed(statsRecord.getKeys())) {
                                    // 如果抢占失败（Key 已存在），判断是否已经“处理完成”
                                    if (messageQueueIdempotentHandler.isAccomplish(statsRecord.getKeys())) {
                                        return; // 已完成，跳过
                                    }
                                    // 既没抢到锁，也没完成，说明状态异常（可能别的线程卡死了），抛异常触发可能的重试逻辑
                                    throw new ServiceException("消息未完成流程，需要消息队列重试");
                                }

                                try {
                                    // 6. 【核心操作】重新归队
                                    // 调用 Service 的 shortLinkStats 方法。
                                    // 注意：这个方法内部会把 statsRecord 封装后发送到【Redis Stream】（主业务队列）。
                                    // 这样，消息就回到了 ShortLinkStatsSaveConsumer，它会再次尝试获取数据库写锁。
                                    shortLinkService.shortLinkStats(null, null, statsRecord);
                                } catch (Throwable ex) {
                                    // 7. 异常回滚
                                    // 如果发送回 Stream 失败了，删除幂等标识，以便下一次循环或者其他机制能再次处理这条延迟消息
                                    messageQueueIdempotentHandler.delMessageProcessed(statsRecord.getKeys());
                                    log.error("延迟记录短链接监控消费异常", ex);
                                }

                                // 8. 标记完成
                                // 标记这个“延迟处理任务”已完成（注意：不是标记数据库入库完成，而是标记“我已经把它送回主队列了”这件事完成）
                                messageQueueIdempotentHandler.setAccomplish(statsRecord.getKeys());
                                continue; // 继续处理下一条
                            }

                            // 9. 空轮询等待
                            // 如果队列为空，挂起线程，避免 CPU 空转 100%。
                            // 注意：LockSupport.parkUntil(500) 参数是绝对毫秒数（Epoch时间），这里写 500 可能是意图 sleep 500ms。
                            // 但实际上 parkUntil(500) 会立即返回（因为 1970年的时间早过了）。
                            // 实际效果可能是类似 Thread.yield() 或极短的停顿，在高并发下依赖 poll 的非阻塞特性。
                            // 建议理解为：这里意图是“休息一会再查”。
                            LockSupport.parkUntil(500);
                        } catch (Throwable ignored) {
                            // 忽略循环中的非致命异常，保证消费者线程不挂掉
                        }
                    }
                });
    }

    /**
     * TODO Spring Bean 初始化完成后自动调用DelayShortLinkStatsConsumer的onMessage方法
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // 启动后台消费者线程
        onMessage();
    }
}