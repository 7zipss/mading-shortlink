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

package com.nageoffer.shortlink.project.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.Week;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.text.StrBuilder;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.shortlink.project.common.convention.exception.ClientException;
import com.nageoffer.shortlink.project.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.project.common.enums.VailDateTypeEnum;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessLogsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkBrowserStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkDeviceStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkLocaleStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkNetworkStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkOsStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsTodayDO;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkDO;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkGotoDO;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessLogsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkBrowserStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkDeviceStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkLocaleStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkNetworkStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkOsStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkStatsTodayMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import com.nageoffer.shortlink.project.dto.req.ShortLinkCreateReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkPageReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkUpdateReqDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkCreateRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkGroupCountQueryRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkPageRespDTO;
import com.nageoffer.shortlink.project.service.ShortLinkService;
import com.nageoffer.shortlink.project.toolkit.HashUtil;
import com.nageoffer.shortlink.project.toolkit.LinkUtil;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_IS_NULL_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GOTO_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.ShortLinkConstant.AMAP_REMOTE_URL;

/**
 * 短链接接口实现层
 */
/**
 * 短链接核心业务层
 * * LEARNING: [核心架构]
 * 本服务采用了双表策略解决 C 端与 B 端查询维度冲突的问题：
 * 1. 写流程 (createShortLink):
 * - 必须同时写入 t_link (按 gid 分片，服务 B 端) 和 t_link_goto (按 url 分片，服务 C 端)。
 * - 存在双写一致性风险 (当前无 @Transactional)。
 * 2. 读流程 (restoreUrl):
 * - 先查 Redis -> 布隆过滤器 -> t_link_goto (拿 gid) -> t_link (拿详情)。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShortLinkServiceImpl extends ServiceImpl<ShortLinkMapper, ShortLinkDO> implements ShortLinkService {

    private final RBloomFilter<String> shortUriCreateCachePenetrationBloomFilter;
    private final ShortLinkGotoMapper shortLinkGotoMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final LinkAccessStatsMapper linkAccessStatsMapper;
    private final LinkLocaleStatsMapper linkLocaleStatsMapper;
    private final LinkOsStatsMapper linkOsStatsMapper;
    private final LinkBrowserStatsMapper linkBrowserStatsMapper;
    private final LinkAccessLogsMapper linkAccessLogsMapper;
    private final LinkDeviceStatsMapper linkDeviceStatsMapper;
    private final LinkNetworkStatsMapper linkNetworkStatsMapper;
    private final LinkStatsTodayMapper linkStatsTodayMapper;

    @Value("${short-link.stats.locale.amap-key}")
    private String statsLocaleAmapKey;

    @Override
    public ShortLinkCreateRespDTO createShortLink(ShortLinkCreateReqDTO requestParam) {
        String shortLinkSuffix = generateSuffix(requestParam);
        String fullShortUrl = StrBuilder.create(requestParam.getDomain())
                .append("/")
                .append(shortLinkSuffix)
                .toString();
        // 1. 构建主表实体（详细信息）
        ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                .domain(requestParam.getDomain())
                .originUrl(requestParam.getOriginUrl())
                .gid(requestParam.getGid())
                .createdType(requestParam.getCreatedType())
                .validDateType(requestParam.getValidDateType())
                .validDate(requestParam.getValidDate())
                .describe(requestParam.getDescribe())
                .shortUri(shortLinkSuffix)
                .enableStatus(0)
                .totalPv(0)
                .totalUv(0)
                .totalUip(0)
                .fullShortUrl(fullShortUrl)
                .favicon(getFavicon(requestParam.getOriginUrl()))
                .build();
        // 2. 构建路由表实体（仅用于跳转）
        ShortLinkGotoDO linkGotoDO = ShortLinkGotoDO.builder()
                .fullShortUrl(fullShortUrl)
                .gid(requestParam.getGid())
                .build();
        try {
            baseMapper.insert(shortLinkDO);
            shortLinkGotoMapper.insert(linkGotoDO);
        } catch (DuplicateKeyException ex) {
            // TODO: [分库分表架构决策]
            // 这里为什么查询不加 gid？
            // 1. 目的：必须检查【全局唯一性】。如果加了 gid，只能检查当前分片，
            //    会导致不同用户生成相同的 fullShortUrl (如果它们被分到了不同的表)，这是逻辑漏洞。
            // 2. 代价：不加 gid 会触发 ShardingSphere 的【全路由广播】，扫描所有 t_link 表，性能较差。
            // 3. 优化思路：应该查 t_link_goto 表！它按 fullShortUrl 分片，只需查 1 张表。
            LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                    .eq(ShortLinkDO::getFullShortUrl, fullShortUrl);
            ShortLinkDO hasShortLinkDO = baseMapper.selectOne(queryWrapper);
            if (hasShortLinkDO != null) {
                log.warn("短链接：{} 重复入库", fullShortUrl);
                throw new ServiceException("短链接生成重复");
            }
        }
        // 1. 放入 Redis 缓存
        stringRedisTemplate.opsForValue().set(
                String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                requestParam.getOriginUrl(),
                LinkUtil.getLinkCacheValidTime(requestParam.getValidDate()), TimeUnit.MILLISECONDS
        );
        // 2. 放入布隆过滤器
        shortUriCreateCachePenetrationBloomFilter.add(fullShortUrl);
        return ShortLinkCreateRespDTO.builder()
                .fullShortUrl("http://" + shortLinkDO.getFullShortUrl())
                .originUrl(requestParam.getOriginUrl())
                .gid(requestParam.getGid())
                .build();
    }

    /**
     * TODO 修改短链接信息
     *
     * 1. 该方法使用了 @Transactional 事务注解，确保数据库操作的原子性。
     * 2. 如果涉及到分组变更（GID 变化），由于数据库采用了基于 GID 的分库分表策略，
     * 无法直接更新分片键，必须采用“先删除旧分片数据，再插入新分片数据”的策略。
     *
     * @param requestParam 短链接修改请求参数
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void updateShortLink(ShortLinkUpdateReqDTO requestParam) {
        // 1. 验证待修改的短链接是否存在
        // 构建查询条件：根据分组 ID、完整短链接、未删除、启用状态查询
        LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                .eq(ShortLinkDO::getGid, requestParam.getGid())
                .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                .eq(ShortLinkDO::getDelFlag, 0)
                .eq(ShortLinkDO::getEnableStatus, 0);

        // 查询数据库
        ShortLinkDO hasShortLinkDO = baseMapper.selectOne(queryWrapper);

        // 如果查不到记录，抛出客户端异常
        if (hasShortLinkDO == null) {
            throw new ClientException("短链接记录不存在");
        }

        // 2. 构建新的短链接实体对象
        // 这里混合使用了数据库中的旧数据（不可变字段）和请求中的新数据（可变字段）
        ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                .domain(hasShortLinkDO.getDomain()) // 域名保持不变
                .shortUri(hasShortLinkDO.getShortUri()) // 短链接后缀保持不变
                .clickNum(hasShortLinkDO.getClickNum()) // 点击量保持不变
                .favicon(hasShortLinkDO.getFavicon()) // 图标保持不变
                .createdType(hasShortLinkDO.getCreatedType()) // 创建类型保持不变
                .gid(requestParam.getGid()) // 设置新的分组 ID
                .originUrl(requestParam.getOriginUrl()) // 设置新的原始链接
                .describe(requestParam.getDescribe()) // 设置新的描述
                .validDateType(requestParam.getValidDateType()) // 设置新的有效期类型
                .validDate(requestParam.getValidDate()) // 设置新的有效期时间
                .build();

        // 3. 判断是否发生了分组变更
        if (Objects.equals(hasShortLinkDO.getGid(), requestParam.getGid())) {
            // ==================== 情况 A：分组未变更 ====================
            // 直接在当前分片表中更新数据即可

            LambdaUpdateWrapper<ShortLinkDO> updateWrapper = Wrappers.lambdaUpdate(ShortLinkDO.class)
                    .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                    .eq(ShortLinkDO::getGid, requestParam.getGid())
                    .eq(ShortLinkDO::getDelFlag, 0)
                    .eq(ShortLinkDO::getEnableStatus, 0)
                    // 特殊逻辑：如果有效期类型改为“永久有效”(0)，则将数据库中的 valid_date 字段置为 NULL
                    .set(Objects.equals(requestParam.getValidDateType(), VailDateTypeEnum.PERMANENT.getType()), ShortLinkDO::getValidDate, null);

            // 执行更新操作
            baseMapper.update(shortLinkDO, updateWrapper);
        } else {
            // ==================== 情况 B：分组已变更 ====================
            // 重点：由于数据库分片键（Sharding Key）是 GID，修改 GID 意味着数据需要迁移到另一张表或另一个库。
            // TODO ShardingSphere 不支持直接更新分片键，因此需要“先删后增”。

            // 3.1 删除旧分组中的数据
            LambdaUpdateWrapper<ShortLinkDO> updateWrapper = Wrappers.lambdaUpdate(ShortLinkDO.class)
                    .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                    .eq(ShortLinkDO::getGid, hasShortLinkDO.getGid()) // 使用旧的 GID 定位数据
                    .eq(ShortLinkDO::getDelFlag, 0)
                    .eq(ShortLinkDO::getEnableStatus, 0);
            // 物理删除（或者根据业务需求做逻辑删除，这里调用的是 delete 方法，通常是物理删除）
            baseMapper.delete(updateWrapper);

            // 3.2 在新分组中插入数据
            // 此时 shortLinkDO 中的 gid 已经是 requestParam 中新的 GID 了，会自动路由到新分片
            baseMapper.insert(shortLinkDO);
            // TODO 对于修改分组（GID），当前的 updateShortLink 代码确实应该修改 goto 表但没有修改，这属于代码逻辑上的缺失或 Bug。在完善的生产环境中，这里应该同步更新 t_link_goto 中的 gid 字段
        }
    }

    /**
     * TODO 分页查询短链接
     *
     * 1. 调用 Mapper 层执行自定义 SQL 查询。
     * SQL 逻辑：t_link 表左连接 t_link_stats_today 表，同时获取短链接信息和今日统计数据。
     * 2. 将数据库实体对象 (ShortLinkDO) 转换为响应对象 (ShortLinkPageRespDTO)。
     * 3. 对部分字段进行增强处理（如拼接协议头）。
     *
     * @param requestParam 分页请求参数（包含页码、页大小、分组ID、排序标识）
     * @return 分页返回结果（包含列表数据和分页元数据）
     */
    @Override
    public IPage<ShortLinkPageRespDTO> pageShortLink(ShortLinkPageReqDTO requestParam) {
        // 1. 执行数据库查询
        // 这里没有使用 MyBatis-Plus 默认的 selectPage，而是调用了自定义的 pageLink 方法。
        // 原因是：默认查询无法关联 't_link_stats_today' 表来获取 "今日PV/UV/UIP" 数据，也无法实现基于统计数据的排序。
        // 具体的 SQL 逻辑定义在 resources/mapper/LinkMapper.xml 中。
        IPage<ShortLinkDO> resultPage = baseMapper.pageLink(requestParam);

        // 2. 数据转换与增强
        // 将 DO (Data Object) 转换为 DTO (Data Transfer Object) 以返回给前端
        return resultPage.convert(each -> {
            // 使用 BeanUtil 工具类进行属性对拷（同名属性复制）
            ShortLinkPageRespDTO result = BeanUtil.toBean(each, ShortLinkPageRespDTO.class);

            // 3. 字段增强处理
            // 数据库中存储的 domain 字段通常不包含协议头（例如 "google.com"）
            // 前端跳转需要完整的 URL，因此这里统一拼接 "http://"
            result.setDomain("http://" + result.getDomain());

            return result;
        });
    }

    /**
     * TODO 查询指定分组下的短链接数量
     *
     * 应用场景：
     * 在用户查看分组列表时，需要展示每个分组下有多少个有效的短链接。
     * 为了避免“N+1”查询问题（即查询N个分组需要发起N+1次数据库调用），
     * 这里采用了 SQL 的 IN 子句和 GROUP BY 聚合查询，一次性查出所有指定分组的统计数据。
     *
     * @param requestParam 分组 ID (GID) 列表
     * @return 分组短链接数量查询结果列表
     */
    @Override
    public List<ShortLinkGroupCountQueryRespDTO> listGroupShortLinkCount(List<String> requestParam) {
        // 1. 构建查询条件
        // 使用 QueryWrapper 而不是 LambdaQueryWrapper，因为需要自定义 select 聚合函数 (count(*))
        QueryWrapper<ShortLinkDO> queryWrapper = Wrappers.query(new ShortLinkDO())
                // 指定查询列：分组标识(gid) 和 该分组下的记录总数(shortLinkCount)
                .select("gid as gid, count(*) as shortLinkCount")
                // 使用 IN 语句限定查询范围，只查询传入的那几个分组
                .in("gid", requestParam)
                // 过滤条件：只统计状态为“启用”的短链接 (0:启用, 1:未启用)
                .eq("enable_status", 0)
                // 显式排除已逻辑删除的记录（如果未配置全局逻辑删除插件，建议显式加上此条件）
                // .eq("del_flag", 0)
                // 按分组标识进行聚合，这样 count(*) 统计的就是每个分组内的数量
                .groupBy("gid");

        // 2. 执行数据库查询
        // selectMaps 返回的是 List<Map<String, Object>>
        // Map 的 Key 是数据库字段名（或别名），Value 是字段值
        // 例如：[ {gid: "xGy7s1", shortLinkCount: 10}, {gid: "aB3d9z", shortLinkCount: 5} ]
        List<Map<String, Object>> shortLinkDOList = baseMapper.selectMaps(queryWrapper);

        // 3. 数据转换
        // 使用 BeanUtil 将 Map 列表直接映射为 DTO (Data Transfer Object) 列表
        // 只要 Map 的 Key (gid, shortLinkCount) 与 DTO 的属性名一致，即可自动填充
        return BeanUtil.copyToList(shortLinkDOList, ShortLinkGroupCountQueryRespDTO.class);
    }

    /**
     * TODO 短链接跳转原始链接（核心接口）
     * LEARNING: [高性能架构]
     * 该接口承载了 C 端海量并发请求，采用了多级缓存架构来保护数据库：
     * 1. 缓存查询：Redis -> 命中即返回 (高性能路径)
     * 2. 过滤拦截：布隆过滤器 -> 拦截不存在的短链 (防止缓存穿透)
     * 3. 空值缓存：Redis 存储空对象 -> 二次拦截误判请求
     * 4. 并发控制：分布式锁 (Redisson) + 双重检查锁 (DCL) -> 防止缓存击穿
     * 5. 数据库路由：路由表 (t_link_goto) -> 主表 (t_link) -> 解决分库分表查询难题
     */
    @SneakyThrows
    @Override
    public void restoreUrl(String shortUri, ServletRequest request, ServletResponse response) {
        String serverName = request.getServerName();
        String fullShortUrl = serverName + "/" + shortUri;

        /*
---------------------------------------------------------第一阶段：缓存层与过滤层-------------------------------------------------------
         */
        // LEARNING: [第一道防线] 缓存优先
        // 绝大多数正常请求应该在这里被拦截并返回，不会打到数据库。
        String originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
        if (StrUtil.isNotBlank(originalLink)) {
            shortLinkStats(fullShortUrl, null, request, response);
            ((HttpServletResponse) response).sendRedirect(originalLink);
            return;
        }
        // TODO 缓存穿透: [第二道防线] 布隆过滤器 (Bloom Filter)
        // 用于防止【缓存穿透】。如果布隆过滤器说“不存在”，那数据库里一定没有，直接返回 404。
        // 注意：布隆过滤器存在误判率（说存在，可能实际上不存在），所以后续还需要空值缓存兜底。
        boolean contains = shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl);
        if (!contains) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }
        // TODO 缓存穿透: [第三道防线] 空值缓存 (Null Object Pattern)
        // 针对布隆过滤器误判的请求，或者数据库确实删除了的短链，则在 Redis 中会存一个 "-" 标记。
        // 再次请求时，直接走这里返回 404，不再查库。
        String gotoIsNullShortLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl));
        if (StrUtil.isNotBlank(gotoIsNullShortLink)) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }
/*
---------------------------------------------------第二阶段：并发控制与数据库路由---------------------------------------------------------------------------------------------------------
 */
        // TODO：缓存击穿（穿透）: [第四道防线] 分布式锁 (Redisson)
        // 场景：热点 Key 突然过期（缓存击穿），瞬间涌入大量请求。
        // 策略：只允许 1 个线程去查库构建缓存，其他线程等待。
        RLock lock = redissonClient.getLock(String.format(LOCK_GOTO_SHORT_LINK_KEY, fullShortUrl));
        lock.lock();
        try {
            // LEARNING: [双重检查锁 (DCL)]
            // 获取锁后，必须再次查询 Redis。
            // 因为在当前线程等待锁的过程中，持有锁的前一个线程可能已经把数据加载到 Redis 了。
            originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
            if (StrUtil.isNotBlank(originalLink)) {
                shortLinkStats(fullShortUrl, null, request, response);
                ((HttpServletResponse) response).sendRedirect(originalLink);
                return;
            }
            // LEARNING: [核心路由策略 - 步骤 1]
            // 问题：t_link 表按 GID 分片，只有 fullShortUrl 无法直接查询 t_link (否则需要扫描全表)。
            // 解决：先查 t_link_goto 表 (路由表，按 fullShortUrl 分片)，获取 GID。
            LambdaQueryWrapper<ShortLinkGotoDO> linkGotoQueryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                    .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
            ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(linkGotoQueryWrapper);
            if (shortLinkGotoDO == null) {
                // 数据库也没查到，说明是布隆过滤器误判了，或者数据被删除了。
                // TODO 缓存穿透：存入空值缓存，防止下次再穿透到数据库。
                stringRedisTemplate.opsForValue().set(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl), "-", 30, TimeUnit.MINUTES);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            // LEARNING: [核心路由策略 - 步骤 2]
            // 拿到 GID 后，携带 GID 去查询主表 t_link。
            // 此时 ShardingSphere 可以根据 GID 精准路由到具体的物理表 (ds_0.t_link_x)，性能极高。
            LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                    .eq(ShortLinkDO::getGid, shortLinkGotoDO.getGid())
                    .eq(ShortLinkDO::getFullShortUrl, fullShortUrl)
                    .eq(ShortLinkDO::getDelFlag, 0)
                    .eq(ShortLinkDO::getEnableStatus, 0);
            ShortLinkDO shortLinkDO = baseMapper.selectOne(queryWrapper);
            // TODO 缓存穿透：数据库未查到（概率小），短链接过期，存入空值缓存
            if (shortLinkDO == null || shortLinkDO.getValidDate().before(new Date())) {
                stringRedisTemplate.opsForValue().set(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl), "-", 30, TimeUnit.MINUTES);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            // redis中缓存短链
            stringRedisTemplate.opsForValue().set(
                    String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                    shortLinkDO.getOriginUrl(),
                    LinkUtil.getLinkCacheValidTime(shortLinkDO.getValidDate()), TimeUnit.MILLISECONDS
            );
            shortLinkStats(fullShortUrl, shortLinkDO.getGid(), request, response);
            ((HttpServletResponse) response).sendRedirect(shortLinkDO.getOriginUrl());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 短链接访问统计核心方法
     * * @param fullShortUrl 完整短链接
     * @param gid 分组标识
     * @param request HTTP 请求对象
     * @param response HTTP 响应对象
     */
    private void shortLinkStats(String fullShortUrl, String gid, ServletRequest request, ServletResponse response) {
        // 原子布尔值，用于标记是否为新访客（UV）
        // TODO 使用 AtomicBoolean 是为了解决 Lambda 表达式的变量捕获限制。
        // 因为 Lambda 内部只能引用 final 或 effectively final 的局部变量，不能直接修改 boolean 基本类型。
        // 使用 AtomicBoolean 可以在 Lambda 内部通过 set() 修改其值，从而将状态传递到外部。
        AtomicBoolean uvFirstFlag = new AtomicBoolean();
        // 获取请求中的 Cookies
        Cookie[] cookies = ((HttpServletRequest) request).getCookies();

        try {
            // 原子引用，用于存储 UV 标识（UUID）
            // TODO 同理，使用 AtomicReference 是为了在 Lambda 内部给 String 类型变量赋值。
            // String 是不可变的，且作为局部变量在 Lambda 中不能被重新指向新的对象。
            AtomicReference<String> uv = new AtomicReference<>();

            // 定义一个 Runnable 任务：用于在没有 UV Cookie 时生成新的 UV 标识并设置到 Cookie 中
            Runnable addResponseCookieTask = () -> {
                // 生成一个新的 UUID 作为用户标识
                uv.set(UUID.fastUUID().toString());
                // 创建 Cookie
                Cookie uvCookie = new Cookie("uv", uv.get());
                // 设置 Cookie 有效期为 30 天
                uvCookie.setMaxAge(60 * 60 * 24 * 30);
                // 设置 Cookie 路径为当前短链接的路径后缀
                uvCookie.setPath(StrUtil.sub(fullShortUrl, fullShortUrl.indexOf("/"), fullShortUrl.length()));
                // 将 Cookie 添加到响应中
                ((HttpServletResponse) response).addCookie(uvCookie);
                // 标记为新访客
                uvFirstFlag.set(Boolean.TRUE);
                // 将新的 UV 标识存入 Redis Set 中，用于去重统计
                stringRedisTemplate.opsForSet().add("short-link:stats:uv:" + fullShortUrl, uv.get());
            };

            // 如果请求中包含 Cookies
            if (ArrayUtil.isNotEmpty(cookies)) {
                // 遍历 Cookies 查找名为 "uv" 的 Cookie
                Arrays.stream(cookies)
                        .filter(each -> Objects.equals(each.getName(), "uv"))
                        .findFirst()
                        .map(Cookie::getValue)
                        .ifPresentOrElse(each -> {
                            // 如果找到了 "uv" Cookie
                            uv.set(each);
                            // 尝试将 UV 标识存入 Redis Set
                            // 如果返回值为非 null 且大于 0，说明 Redis 中之前不存在该值，即为新访客
                            Long uvAdded = stringRedisTemplate.opsForSet().add("short-link:stats:uv:" + fullShortUrl, each);
                            uvFirstFlag.set(uvAdded != null && uvAdded > 0L);
                        }, addResponseCookieTask); // 如果没找到 "uv" Cookie，执行添加 Cookie 的任务
            } else {
                // 如果请求中完全没有 Cookies，直接执行添加 Cookie 的任务
                addResponseCookieTask.run();
            }

            // 获取用户真实 IP 地址
            String remoteAddr = LinkUtil.getActualIp(((HttpServletRequest) request));
            // 尝试将 IP 存入 Redis Set，用于计算 UIP（独立 IP 数）
            Long uipAdded = stringRedisTemplate.opsForSet().add("short-link:stats:uip:" + fullShortUrl, remoteAddr);
            // 如果返回值大于 0，说明是新的独立 IP
            boolean uipFirstFlag = uipAdded != null && uipAdded > 0L;

            // 如果 gid 为空，查询数据库获取该短链接的分组 ID
            if (StrUtil.isBlank(gid)) {
                LambdaQueryWrapper<ShortLinkGotoDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                        .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
                ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(queryWrapper);
                gid = shortLinkGotoDO.getGid();
            }

            // 获取当前时间的小时数
            int hour = DateUtil.hour(new Date(), true);
            // 获取当前是周几
            Week week = DateUtil.dayOfWeekEnum(new Date());
            int weekValue = week.getIso8601Value();

            // 构建基础访问统计实体 (PV, UV, UIP)
            LinkAccessStatsDO linkAccessStatsDO = LinkAccessStatsDO.builder()
                    .pv(1) // 本次请求算作 1 个 PV
                    .uv(uvFirstFlag.get() ? 1 : 0) // 如果是新访客，UV + 1
                    .uip(uipFirstFlag ? 1 : 0) // 如果是新 IP，UIP + 1
                    .hour(hour)
                    .weekday(weekValue)
                    .fullShortUrl(fullShortUrl)
                    .gid(gid)
                    .date(new Date())
                    .build();
            // 插入或更新基础访问统计数据
            linkAccessStatsMapper.shortLinkStats(linkAccessStatsDO);

            // 准备调用高德地图 API 获取地理位置信息
            Map<String, Object> localeParamMap = new HashMap<>();
            localeParamMap.put("key", statsLocaleAmapKey); // 高德 API Key
            localeParamMap.put("ip", remoteAddr); // 用户 IP
            // 发起 HTTP 请求
            String localeResultStr = HttpUtil.get(AMAP_REMOTE_URL, localeParamMap);
            // 解析返回的 JSON
            JSONObject localeResultObj = JSON.parseObject(localeResultStr);
            String infoCode = localeResultObj.getString("infocode");

            String actualProvince;
            String actualCity;

            // 如果 API 调用成功 (infocode 为 10000)
            if (StrUtil.isNotBlank(infoCode) && StrUtil.equals(infoCode, "10000")) {
                String province = localeResultObj.getString("province");
                // 判断省份是否为空（高德有时返回 "[]"）
                boolean unknownFlag = StrUtil.equals(province, "[]");

                // 构建地区统计实体
                LinkLocaleStatsDO linkLocaleStatsDO = LinkLocaleStatsDO.builder()
                        .province(actualProvince = unknownFlag ? "未知" : province)
                        .city(actualCity = unknownFlag ? "未知" : localeResultObj.getString("city"))
                        .adcode(unknownFlag ? "未知" : localeResultObj.getString("adcode"))
                        .cnt(1)
                        .fullShortUrl(fullShortUrl)
                        .country("中国")
                        .gid(gid)
                        .date(new Date())
                        .build();
                // 插入或更新地区统计数据
                linkLocaleStatsMapper.shortLinkLocaleState(linkLocaleStatsDO);

                // 获取并统计操作系统信息
                String os = LinkUtil.getOs(((HttpServletRequest) request));
                LinkOsStatsDO linkOsStatsDO = LinkOsStatsDO.builder()
                        .os(os)
                        .cnt(1)
                        .gid(gid)
                        .fullShortUrl(fullShortUrl)
                        .date(new Date())
                        .build();
                linkOsStatsMapper.shortLinkOsState(linkOsStatsDO);

                // 获取并统计浏览器信息
                String browser = LinkUtil.getBrowser(((HttpServletRequest) request));
                LinkBrowserStatsDO linkBrowserStatsDO = LinkBrowserStatsDO.builder()
                            .browser(browser)
                        .cnt(1)
                        .gid(gid)
                        .fullShortUrl(fullShortUrl)
                        .date(new Date())
                        .build();
                linkBrowserStatsMapper.shortLinkBrowserState(linkBrowserStatsDO);

                // 获取并统计设备类型（如 Mobile/PC）
                String device = LinkUtil.getDevice(((HttpServletRequest) request));
                LinkDeviceStatsDO linkDeviceStatsDO = LinkDeviceStatsDO.builder()
                        .device(device)
                        .cnt(1)
                        .gid(gid)
                        .fullShortUrl(fullShortUrl)
                        .date(new Date())
                        .build();
                linkDeviceStatsMapper.shortLinkDeviceState(linkDeviceStatsDO);

                // 获取并统计网络类型（如 Wifi/4G）
                String network = LinkUtil.getNetwork(((HttpServletRequest) request));
                LinkNetworkStatsDO linkNetworkStatsDO = LinkNetworkStatsDO.builder()
                        .network(network)
                        .cnt(1)
                        .gid(gid)
                        .fullShortUrl(fullShortUrl)
                        .date(new Date())
                        .build();
                linkNetworkStatsMapper.shortLinkNetworkState(linkNetworkStatsDO);

                // 记录详细的访问日志 (t_link_access_logs)
                LinkAccessLogsDO linkAccessLogsDO = LinkAccessLogsDO.builder()
                        .user(uv.get()) // 用户 UV 标识
                        .ip(remoteAddr)
                        .browser(browser)
                        .os(os)
                        .network(network)
                        .device(device)
                        .locale(StrUtil.join("-", "中国", actualProvince, actualCity))
                        .gid(gid)
                        .fullShortUrl(fullShortUrl)
                        .build();
                linkAccessLogsMapper.insert(linkAccessLogsDO);

                // 更新 t_link 表的总统计数据 (total_pv, total_uv, total_uip)
                baseMapper.incrementStats(gid, fullShortUrl, 1, uvFirstFlag.get() ? 1 : 0, uipFirstFlag ? 1 : 0);

                // 更新今日统计表 (t_link_stats_today)
                LinkStatsTodayDO linkStatsTodayDO = LinkStatsTodayDO.builder()
                        .todayPv(1)
                        .todayUv(uvFirstFlag.get() ? 1 : 0)
                        .todayUip(uipFirstFlag ? 1 : 0)
                        .gid(gid)
                        .fullShortUrl(fullShortUrl)
                        .date(new Date())
                        .build();
                linkStatsTodayMapper.shortLinkTodayState(linkStatsTodayDO);
            }
        } catch (Throwable ex) {
            // 捕获所有异常，确保统计逻辑的失败不会影响短链接的正常跳转
            log.error("短链接访问量统计异常", ex);
        }
    }

    private String generateSuffix(ShortLinkCreateReqDTO requestParam) {
        int customGenerateCount = 0;
        String shorUri;
        while (true) {
            // 防止死循环
            if (customGenerateCount > 10) {
                throw new ServiceException("短链接频繁生成，请稍后再试");
            }
            String originUrl = requestParam.getOriginUrl();
            // 关键！如果有冲突，给原始URL加点“盐”（时间戳），重新Hash
            originUrl += System.currentTimeMillis();
            shorUri = HashUtil.hashToBase62(originUrl);
            // 如果布隆过滤器说“不存在”，那就说明这个短码可用，跳出循环
            if (!shortUriCreateCachePenetrationBloomFilter.contains(requestParam.getDomain() + "/" + shorUri)) {
                break;
            }
            customGenerateCount++;
        }
        return shorUri;
    }

    @SneakyThrows
    private String getFavicon(String url) {
        URL targetUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) targetUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        int responseCode = connection.getResponseCode();
        if (HttpURLConnection.HTTP_OK == responseCode) {
            Document document = Jsoup.connect(url).get();
            Element faviconLink = document.select("link[rel~=(?i)^(shortcut )?icon]").first();
            if (faviconLink != null) {
                return faviconLink.attr("abs:href");
            }
        }
        return null;
    }
}
