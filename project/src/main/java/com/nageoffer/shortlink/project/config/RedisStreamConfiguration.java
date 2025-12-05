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

import com.nageoffer.shortlink.project.mq.consumer.ShortLinkStatsSaveConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_GROUP_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_TOPIC_KEY;

/**
 * Redis Stream 消息队列配置类
 * <p>
 * 作用：初始化 Stream 监听容器，配置线程池，并将消费者绑定到具体的 Stream Topic 上。
 */
@Configuration
@RequiredArgsConstructor
public class RedisStreamConfiguration {

    private final RedisConnectionFactory redisConnectionFactory;
    private final ShortLinkStatsSaveConsumer shortLinkStatsSaveConsumer;

    /**
     * 创建一个自定义线程池，专门用于处理 Stream 消息消费
     * <p>
     * 为什么要自定义？
     * 1. 避免占用 Spring Boot 默认的公共线程池，防止 MQ 消费阻塞影响其他异步任务。
     * 2. 可以给线程命名（stream_consumer_...），方便排查问题。
     * 3. 设置为 Daemon 线程，随 JVM 关闭而自动销毁。
     */
    @Bean
    public ExecutorService asyncStreamConsumer() {
        AtomicInteger index = new AtomicInteger();
        int processors = Runtime.getRuntime().availableProcessors(); // 获取 CPU 核心数
        return new ThreadPoolExecutor(processors,
                processors + processors >> 1, // 最大线程数 = 核心数 + 核心数/2
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("stream_consumer_short-link_stats_" + index.incrementAndGet());
                    thread.setDaemon(true); // 守护线程
                    return thread;
                }
        );
    }

    /**
     * 创建 Stream 消息监听容器
     * <p>
     * 这是 Redis Stream 消费的核心驱动器，它会在后台无限循环拉取消息。
     *
     * @param asyncStreamConsumer 上面定义的自定义线程池
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer(ExecutorService asyncStreamConsumer) {
        // 1. 配置容器选项
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        // 【重要】设置一次拉取多少条消息。
                        // 如果设置太大，处理不过来会导致阻塞；设置太小，网络交互频繁效率低。这里设为 10。
                        .batchSize(10)
                        // 指定执行消费任务的线程池
                        .executor(asyncStreamConsumer)
                        // 【重要】设置长轮询阻塞时间 (Block)。
                        // 如果 Stream 里没消息，Redis 会 hold 住连接 3秒，直到有消息或超时。
                        // 这比短轮询（没消息立马返回空）更节省 CPU 和网络资源。
                        // 注意：这个值不能大于 Redis 客户端的连接超时时间 (ReadTimeout)。
                        .pollTimeout(Duration.ofSeconds(3))
                        .build();

        // 2. 创建容器实例
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer =
                StreamMessageListenerContainer.create(redisConnectionFactory, options);

        // 3. 绑定消费者与 Topic
        // receiveAutoAck: 接收消息。虽然名字带 AutoAck，但实际是否 ACK 取决于业务逻辑。
        // 在本项目中，Consumer 处理完消息后会手动执行 delete 操作，起到了确认作用。
        streamMessageListenerContainer.receiveAutoAck(
                // 定义消费者信息：Group 名 + Consumer 名
                // 对应 Redis 命令：XGROUP CREATE ...
                Consumer.from(SHORT_LINK_STATS_STREAM_GROUP_KEY, "stats-consumer"),

                // 定义监听目标：Topic Key + 读取偏移量
                // ReadOffset.lastConsumed(): 从“上次消费完的位置”继续读取，这是消费组的标准行为。
                // 对应 Redis 命令：XREADGROUP GROUP ... STREAMS key >
                StreamOffset.create(SHORT_LINK_STATS_STREAM_TOPIC_KEY, ReadOffset.lastConsumed()),

                // 指定真正的业务处理类（监听器）
                shortLinkStatsSaveConsumer
        );

        return streamMessageListenerContainer;
    }
}