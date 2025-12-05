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

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.Week;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.shortlink.project.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.project.dao.entity.*;
import com.nageoffer.shortlink.project.dao.mapper.*;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.nageoffer.shortlink.project.mq.idempotent.MessageQueueIdempotentHandler;
import com.nageoffer.shortlink.project.mq.producer.DelayShortLinkStatsProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GID_UPDATE_KEY;
import static com.nageoffer.shortlink.project.common.constant.ShortLinkConstant.AMAP_REMOTE_URL;

/**
 * 短链接监控状态保存消息队列消费者
 * <p>
 * 负责从 Redis Stream 消费短链接访问数据，并将其持久化到数据库的各类统计表中。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShortLinkStatsSaveConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    // 注入各类 Mapper，用于操作数据库表
    private final ShortLinkMapper shortLinkMapper;
    private final ShortLinkGotoMapper shortLinkGotoMapper;
    private final RedissonClient redissonClient;
    private final LinkAccessStatsMapper linkAccessStatsMapper;
    private final LinkLocaleStatsMapper linkLocaleStatsMapper;
    private final LinkOsStatsMapper linkOsStatsMapper;
    private final LinkBrowserStatsMapper linkBrowserStatsMapper;
    private final LinkAccessLogsMapper linkAccessLogsMapper;
    private final LinkDeviceStatsMapper linkDeviceStatsMapper;
    private final LinkNetworkStatsMapper linkNetworkStatsMapper;
    private final LinkStatsTodayMapper linkStatsTodayMapper;

    // 注入延迟队列生产者，用于处理锁竞争失败时的重试
    private final DelayShortLinkStatsProducer delayShortLinkStatsProducer;
    private final StringRedisTemplate stringRedisTemplate;

    // 注入幂等处理器，防止重复消费
    private final MessageQueueIdempotentHandler messageQueueIdempotentHandler;

    @Value("${short-link.stats.locale.amap-key}")
    private String statsLocaleAmapKey;

    /**
     * 消息监听核心方法
     * 当 Redis Stream 有新消息时，此方法会被自动调用
     *
     * @param message 接收到的消息，包含 Stream 名、消息 ID 和消息体(Map)
     *                Stream 名 (Stream Key是 Redis 中存储这个队列的 Key) -> SHORT_LINK_STATS_STREAM_TOPIC_KEY = "short-link:stats-stream"
     *                消息 ID (Record ID) -> Redis Stream 自动生成的全局唯一 ID，格式通常为 <毫秒时间戳>-<序列号>（例如 1678888888888-0）。它保证了消息的顺序性
     *                消息体 (Body) -> producerMap
     */
    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        String stream = message.getStream();
        RecordId id = message.getId();
        // 1. 幂等性检查
        // 检查该消息 ID 是否已经被处理过。如果已处理，则直接跳过，防止重复统计 PV/UV。
        if (!messageQueueIdempotentHandler.isMessageProcessed(id.toString())) {
            // 二次检查：防止并发场景下的极小概率重复，或者处理异常情况
            if (messageQueueIdempotentHandler.isAccomplish(id.toString())) {
                return;
            }
            throw new ServiceException("消息未完成流程，需要消息队列重试");
        }

        try {
            // 2. 解析消息体
            Map<String, String> producerMap = message.getValue();
            String fullShortUrl = producerMap.get("fullShortUrl");

            // 3. 执行核心业务逻辑
            if (StrUtil.isNotBlank(fullShortUrl)) {
                String gid = producerMap.get("gid");
                // 将 JSON 字符串反序列化为统计实体对象
                ShortLinkStatsRecordDTO statsRecord = JSON.parseObject(producerMap.get("statsRecord"), ShortLinkStatsRecordDTO.class);
                // TODO 调用实际的保存方法
                actualSaveShortLinkStats(fullShortUrl, gid, statsRecord);
            }

            // 4. 确认消费完成（删除在"short-link:stats-stream"的消息，而不是删除"short-link:idempotent:"+messageId）
            // 注意：这里直接删除了消息，而不是仅 ACK。根据业务需求，处理完就删可以节省 Redis 内存。
            stringRedisTemplate.opsForStream().delete(Objects.requireNonNull(stream), id.getValue());

        } catch (Throwable ex) {
            // 5. 异常处理
            // 如果处理过程中发生异常（如数据库宕机），删除幂等标识，以便这条消息能被重新消费（或进入死信队列逻辑）
            messageQueueIdempotentHandler.delMessageProcessed(id.toString());
            log.error("记录短链接监控消费异常", ex);
        }

        // 6. 标记消息流程彻底完成
        messageQueueIdempotentHandler.setAccomplish(id.toString());
    }

    /**
     * 实际的数据库保存逻辑
     * 将统计数据分散写入到 8+1 张数据库表中
     */
    /**
     * 真正完成
     *     t_link_access_logs、
     *     t_link_access_stats、
     *     t_link_network_stats、
     *     t_link_device_stats、
     *     t_link_locale_stats、
     *     t_link_browser_stats、
     *     t_link_os_stats、
     *     t_link_stats_today
     *     t_link
     * 9个表的填充与更新
     */

    public void actualSaveShortLinkStats(String fullShortUrl, String gid, ShortLinkStatsRecordDTO statsRecord) {
        // 兜底：如果 map 里没有 url，从对象里取
        fullShortUrl = Optional.ofNullable(fullShortUrl).orElse(statsRecord.getFullShortUrl());

        // 1. 获取分布式读写锁
        // 针对当前短链接加锁。这里获取的是【读锁】。
        // TODO 场景：如果此时有人正在修改(监控与统计表等其他表的)分组（gid）（持有写锁），则 tryLock 会失败。
        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(String.format(LOCK_GID_UPDATE_KEY, fullShortUrl));
        RLock rLock = readWriteLock.readLock();

        // 2. 尝试加锁
        if (!rLock.tryLock()) {
            // 加锁失败，说明该短链接正在进行分组迁移等写操作。
            // 为了保证数据一致性（别把统计数据写错了分组），不能现在强行写库。
            // TODO 策略：发送到【延迟队列】，过一会（如5秒后）再重试。
            delayShortLinkStatsProducer.send(statsRecord);
            return;
        }

        try {
            // 3. 补充 GID
            // 如果消息里没传 GID（极少情况），去路由表查一下。因为分表必须要有 GID。
            if (StrUtil.isBlank(gid)) {
                LambdaQueryWrapper<ShortLinkGotoDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                        .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
                ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(queryWrapper);
                gid = shortLinkGotoDO.getGid();
            }

            // 4. 准备基础时间维度数据
            int hour = DateUtil.hour(new Date(), true); // 当前小时 (0-23)
            Week week = DateUtil.dayOfWeekEnum(new Date());
            int weekValue = week.getIso8601Value(); // 当前周几 (1-7)

            // 5. 【表1】基础访问统计 (t_link_access_stats)
            // 记录 PV, UV, UIP 以及按小时、按周几的分布
            LinkAccessStatsDO linkAccessStatsDO = LinkAccessStatsDO.builder()
                    .pv(1)
                    .uv(statsRecord.getUvFirstFlag() ? 1 : 0) // 如果是新 UV 则 +1，否则 +0
                    .uip(statsRecord.getUipFirstFlag() ? 1 : 0) // 如果是新 IP 则 +1，否则 +0
                    .hour(hour)
                    .weekday(weekValue)
                    .fullShortUrl(fullShortUrl)
                    .gid(gid)
                    .date(new Date())
                    .build();
            linkAccessStatsMapper.shortLinkStats(linkAccessStatsDO);

            // 6. 【表2】地区统计 (t_link_locale_stats)
            // 调用高德 API 将 IP 解析为 省份/城市
            Map<String, Object> localeParamMap = new HashMap<>();
            localeParamMap.put("key", statsLocaleAmapKey);
            localeParamMap.put("ip", statsRecord.getRemoteAddr());
            String localeResultStr = HttpUtil.get(AMAP_REMOTE_URL, localeParamMap);
            JSONObject localeResultObj = JSON.parseObject(localeResultStr);
            String infoCode = localeResultObj.getString("infocode");
            String actualProvince = "未知";
            String actualCity = "未知";
            // 解析成功
            if (StrUtil.isNotBlank(infoCode) && StrUtil.equals(infoCode, "10000")) {
                String province = localeResultObj.getString("province");
                boolean unknownFlag = StrUtil.equals(province, "[]");
                LinkLocaleStatsDO linkLocaleStatsDO = LinkLocaleStatsDO.builder()
                        .province(actualProvince = unknownFlag ? actualProvince : province)
                        .city(actualCity = unknownFlag ? actualCity : localeResultObj.getString("city"))
                        .adcode(unknownFlag ? "未知" : localeResultObj.getString("adcode"))
                        .cnt(1)
                        .fullShortUrl(fullShortUrl)
                        .country("中国")
                        .gid(gid)
                        .date(new Date())
                        .build();
                linkLocaleStatsMapper.shortLinkLocaleState(linkLocaleStatsDO);
            }

            // 7. 【表3】操作系统统计 (t_link_os_stats)
            LinkOsStatsDO linkOsStatsDO = LinkOsStatsDO.builder()
                    .os(statsRecord.getOs())
                    .cnt(1)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkOsStatsMapper.shortLinkOsState(linkOsStatsDO);

            // 8. 【表4】浏览器统计 (t_link_browser_stats)
            LinkBrowserStatsDO linkBrowserStatsDO = LinkBrowserStatsDO.builder()
                    .browser(statsRecord.getBrowser())
                    .cnt(1)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkBrowserStatsMapper.shortLinkBrowserState(linkBrowserStatsDO);

            // 9. 【表5】设备类型统计 (t_link_device_stats)
            LinkDeviceStatsDO linkDeviceStatsDO = LinkDeviceStatsDO.builder()
                    .device(statsRecord.getDevice())
                    .cnt(1)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkDeviceStatsMapper.shortLinkDeviceState(linkDeviceStatsDO);

            // 10. 【表6】网络类型统计 (t_link_network_stats)
            LinkNetworkStatsDO linkNetworkStatsDO = LinkNetworkStatsDO.builder()
                    .network(statsRecord.getNetwork())
                    .cnt(1)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkNetworkStatsMapper.shortLinkNetworkState(linkNetworkStatsDO);

            // 11. 【表7】访问日志流水 (t_link_access_logs) -- 每次访问都会产生一条记录 （与【表1】区分开）
            // 记录最详细的单次访问信息，包括具体 IP、用户 Cookie 等
            LinkAccessLogsDO linkAccessLogsDO = LinkAccessLogsDO.builder()
                    .user(statsRecord.getUv())
                    .ip(statsRecord.getRemoteAddr())
                    .browser(statsRecord.getBrowser())
                    .os(statsRecord.getOs())
                    .network(statsRecord.getNetwork())
                    .device(statsRecord.getDevice())
                    .locale(StrUtil.join("-", "中国", actualProvince, actualCity))
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .build();
            linkAccessLogsMapper.insert(linkAccessLogsDO);

            // 12. 【主表更新（表9）】增加 t_link 表的总 PV/UV/UIP 计数
            shortLinkMapper.incrementStats(gid, fullShortUrl, 1, statsRecord.getUvFirstFlag() ? 1 : 0, statsRecord.getUipFirstFlag() ? 1 : 0);

            // 13. 【表8】今日统计 (t_link_stats_today)
            // 单独记录当天的统计数据，用于快速展示“今日数据”图表
            LinkStatsTodayDO linkStatsTodayDO = LinkStatsTodayDO.builder()
                    .todayPv(1)
                    .todayUv(statsRecord.getUvFirstFlag() ? 1 : 0)
                    .todayUip(statsRecord.getUipFirstFlag() ? 1 : 0)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkStatsTodayMapper.shortLinkTodayState(linkStatsTodayDO);

        } catch (Throwable ex) {
            log.error("短链接访问量统计异常", ex);
        } finally {
            // 14. 释放读锁
            rLock.unlock();
        }
    }
}