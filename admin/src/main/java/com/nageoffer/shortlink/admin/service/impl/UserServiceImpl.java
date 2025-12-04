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

package com.nageoffer.shortlink.admin.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.UUID;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.shortlink.admin.common.convention.exception.ClientException;
import com.nageoffer.shortlink.admin.common.enums.UserErrorCodeEnum;
import com.nageoffer.shortlink.admin.dao.entity.UserDO;
import com.nageoffer.shortlink.admin.dao.mapper.UserMapper;
import com.nageoffer.shortlink.admin.dto.req.UserLoginReqDTO;
import com.nageoffer.shortlink.admin.dto.req.UserRegisterReqDTO;
import com.nageoffer.shortlink.admin.dto.req.UserUpdateReqDTO;
import com.nageoffer.shortlink.admin.dto.resp.UserLoginRespDTO;
import com.nageoffer.shortlink.admin.dto.resp.UserRespDTO;
import com.nageoffer.shortlink.admin.service.GroupService;
import com.nageoffer.shortlink.admin.service.UserService;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.BeanUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

import static com.nageoffer.shortlink.admin.common.constant.RedisCacheConstant.LOCK_USER_REGISTER_KEY;
import static com.nageoffer.shortlink.admin.common.enums.UserErrorCodeEnum.USER_EXIST;
import static com.nageoffer.shortlink.admin.common.enums.UserErrorCodeEnum.USER_NAME_EXIST;
import static com.nageoffer.shortlink.admin.common.enums.UserErrorCodeEnum.USER_SAVE_ERROR;

/**
 * 用户接口实现层
 * [亮点]：
 * 1. 使用 Redisson 实现布隆过滤器和分布式锁。
 * 2. 使用 StringRedisTemplate 操作 Redis 缓存。
 * 3. 结合 MyBatis-Plus 简化数据库操作。
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@Service
@RequiredArgsConstructor
public class UserServiceImpl extends ServiceImpl<UserMapper, UserDO> implements UserService {

    // [亮点]：布隆过滤器，用于快速判断用户名是否存在，防止数据库缓存穿透。
    private final RBloomFilter<String> userRegisterCachePenetrationBloomFilter;
    // Redisson 客户端，用于分布式锁。
    private final RedissonClient redissonClient;
    // Spring Data Redis 模板工具。
    private final StringRedisTemplate stringRedisTemplate;
    // 分组服务，用于用户注册时创建默认分组。
    private final GroupService groupService;

    /**
     * 根据用户名查询用户信息
                *
                * @param username 用户名
                * @return 用户返回实体
                */
        @Override
        public UserRespDTO getUserByUsername(String username) {
            // 使用 LambdaQueryWrapper 构造查询条件，避免硬编码字段名。
            LambdaQueryWrapper<UserDO> queryWrapper = Wrappers.lambdaQuery(UserDO.class)
                    .eq(UserDO::getUsername, username);
            UserDO userDO = baseMapper.selectOne(queryWrapper);
        // 如果用户不存在，抛出客户端异常（ClientException）。
        if (userDO == null) {
            throw new ClientException(UserErrorCodeEnum.USER_NULL);
        }
        UserRespDTO result = new UserRespDTO();
        // 使用 BeanUtils 进行对象属性拷贝，将 DO 转换为 DTO 返回给前端。
        BeanUtils.copyProperties(userDO, result);
        return result;
    }

    /**
     * 查询用户名是否存在
     *
     * @param username 用户名
     * @return true: 用户名可用（不存在）; false: 用户名不可用（已存在）
     */
    @Override
    public Boolean hasUsername(String username) {
        // [亮点]：利用布隆过滤器进行初步筛选。
        // 如果布隆过滤器判断不存在，则一定不存在；如果判断存在，则可能存在（误判率极低）。
        // 这里取反，contains 返回 true 表示存在（不可用），所以 !contains 返回 false。
        return !userRegisterCachePenetrationBloomFilter.contains(username);
    }

    /**
     * 注册用户
     *
     * @param requestParam 注册用户请求参数
     */
    @Override
    public void register(UserRegisterReqDTO requestParam) {
        // 1. 先用布隆过滤器快速校验用户名是否已存在
        if (!hasUsername(requestParam.getUsername())) {
            throw new ClientException(USER_NAME_EXIST);
        }

        // 2. [亮点]：分布式锁。
        // TODO 分布式锁：防止在高并发场景下，同一个用户名同时注册导致的问题（虽然数据库有唯一索引，但应用层拦截更优）。
        RLock lock = redissonClient.getLock(LOCK_USER_REGISTER_KEY + requestParam.getUsername());
        try {
            // tryLock 尝试获取锁，如果获取成功则执行注册逻辑。
            if (lock.tryLock()) {
                try {
                    // 3. 插入数据库。
                    int inserted = baseMapper.insert(BeanUtil.toBean(requestParam, UserDO.class));
                    if (inserted < 1) {
                        throw new ClientException(USER_SAVE_ERROR);
                    }
                } catch (DuplicateKeyException ex) {
                    // 捕获数据库唯一索引冲突异常，作为兜底方案。
                    throw new ClientException(USER_EXIST);
                }

                // 4. TODO 用户注册成功后，将用户名加入布隆过滤器。
                userRegisterCachePenetrationBloomFilter.add(requestParam.getUsername());

                // 5. [业务逻辑]：新用户注册成功，自动创建一个默认分组。
                groupService.saveGroup(requestParam.getUsername(), "默认分组");
                return;
            }
            // 获取锁失败，说明有其他线程正在注册该用户名。
            throw new ClientException(USER_NAME_EXIST);
        } finally {
            // 释放锁。
            lock.unlock();
        }
    }

    /**
     * 根据用户名修改用户
     *
     * @param requestParam 修改用户请求参数
     */
    @Override
    public void update(UserUpdateReqDTO requestParam) {
        // TODO 验证当前用户名是否为登录用户 (安全性校验，通常结合 UserContext 做)

        // 构造更新条件：根据用户名更新其他字段。
        LambdaUpdateWrapper<UserDO> updateWrapper = Wrappers.lambdaUpdate(UserDO.class)
                .eq(UserDO::getUsername, requestParam.getUsername());
        // 执行更新。
        baseMapper.update(BeanUtil.toBean(requestParam, UserDO.class), updateWrapper);
    }

    /**
     * 用户登录
     *
     * @param requestParam 用户登录请求参数
     * @return 用户登录返回参数 Token
     */
    @Override
    public UserLoginRespDTO login(UserLoginReqDTO requestParam) {
        // 1. 查询数据库验证用户名和密码。
        LambdaQueryWrapper<UserDO> queryWrapper = Wrappers.lambdaQuery(UserDO.class)
                .eq(UserDO::getUsername, requestParam.getUsername())
                .eq(UserDO::getPassword, requestParam.getPassword())
                .eq(UserDO::getDelFlag, 0); // 确保用户未被删除
        UserDO userDO = baseMapper.selectOne(queryWrapper);
        if (userDO == null) {
            throw new ClientException("用户不存在");
        }

        // 2. TODO 检查用户是否已经登录（幂等性/单点登录限制）。
        // 查看 Redis 中是否已有该用户的登录 Token。
        // key 格式：login_用户名
        Boolean hasLogin = stringRedisTemplate.hasKey("login_" + requestParam.getUsername());
        if (hasLogin != null && hasLogin) {
            throw new ClientException("用户已登录");
        }

        /**
         * 3. TODO Redis Hash 结构存储登录信息。
         * Key：login_用户名
         * HashKey：token (UUID)
         * HashValue：用户信息 JSON 字符串
         * 这种结构方便通过用户名查找 Token，也方便通过 Token 获取用户信息。
         */
        String uuid = UUID.randomUUID().toString();
        stringRedisTemplate.opsForHash().put("login_" + requestParam.getUsername(), uuid, JSON.toJSONString(userDO));

        // 4. 设置过期时间（例如 30 天）。
        stringRedisTemplate.expire("login_" + requestParam.getUsername(), 30L, TimeUnit.DAYS);

        // 返回 Token 给前端。
        return new UserLoginRespDTO(uuid);
    }

    /**
     * 检查用户是否登录
     *
     * @param username 用户名
     * @param token    用户登录 Token
     * @return 用户是否登录标识
     */
    @Override
    public Boolean checkLogin(String username, String token) {
        // 直接从 Redis 中获取，如果能取到值说明 Token 有效且未过期。
        return stringRedisTemplate.opsForHash().get("login_" + username, token) != null;
    }

    /**
     * 退出登录
     *
     * @param username 用户名
     * @param token    用户登录 Token
     */
    @Override
    public void logout(String username, String token) {
        // 1. 验证 Token 是否有效。
        if (checkLogin(username, token)) {
            // 2. 删除 Redis 中的登录 Key，立即使 Token 失效。
            stringRedisTemplate.delete("login_" + username);
            return;
        }
        throw new ClientException("用户Token不存在或用户未登录");
    }
}