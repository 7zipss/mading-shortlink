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
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.shortlink.admin.common.biz.user.UserContext;
import com.nageoffer.shortlink.admin.common.convention.result.Result;
import com.nageoffer.shortlink.admin.dao.entity.GroupDO;
import com.nageoffer.shortlink.admin.dao.mapper.GroupMapper;
import com.nageoffer.shortlink.admin.dto.req.ShortLinkGroupSortReqDTO;
import com.nageoffer.shortlink.admin.dto.req.ShortLinkGroupUpdateReqDTO;
import com.nageoffer.shortlink.admin.dto.resp.ShortLinkGroupRespDTO;
import com.nageoffer.shortlink.admin.remote.ShortLinkRemoteService;
import com.nageoffer.shortlink.admin.remote.dto.resp.ShortLinkGroupCountQueryRespDTO;
import com.nageoffer.shortlink.admin.service.GroupService;
import com.nageoffer.shortlink.admin.toolkit.RandomGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 短链接分组接口实现层
 * [亮点]：继承 ServiceImpl 获得了 MyBatis-Plus 提供的基础 CRUD 能力
 * * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@Slf4j
@Service
public class GroupServiceImpl extends ServiceImpl<GroupMapper, GroupDO> implements GroupService {

    /**
     * 后续重构为 SpringCloud Feign 调用
     * [亮点]：这里模拟了微服务之间的远程调用。在实际微服务架构中，
     * Admin 服务需要去 Project 服务（短链接核心服务）查询短链接的数量。
     * 目前通过匿名内部类简单实现，后续会替换为 OpenFeign。
     */
    ShortLinkRemoteService shortLinkRemoteService = new ShortLinkRemoteService() {
    };

    /**
     * 新增短链接分组（默认使用当前登录用户名）
     */
    @Override
    public void saveGroup(String groupName) {
        // [亮点]：UserContext 是一个 ThreadLocal 封装工具，用于在一次请求中
        // 隐式传递用户信息（从拦截器/过滤器中解析 Token 放入）。
        saveGroup(UserContext.getUsername(), groupName);
    }

    /**
     * 新增短链接分组（指定用户名）
     * [业务逻辑]：生成一个不重复的 6 位随机字符串作为 Group ID (GID)
     */
    @Override
    public void saveGroup(String username, String groupName) {
        String gid;
        // [亮点]：防碰撞逻辑。
        // 循环生成 GID，直到数据库中不存在相同的 GID 为止。
        // 这里的 hasGid 检查是必要的，防止极小概率的哈希冲突。
        do {
            gid = RandomGenerator.generateRandom();
        } while (!hasGid(username, gid));

        GroupDO groupDO = GroupDO.builder()
                .gid(gid)
                .sortOrder(0) // 默认排序权重为 0
                .username(username)
                .name(groupName)
                .build();
        baseMapper.insert(groupDO);
    }

    /**
     * 查询用户短链接分组集合
     * [业务逻辑]：不仅要查出分组列表，还要查出每个分组下有多少个短链接。
     */
    @Override
    public List<ShortLinkGroupRespDTO> listGroup() {
        // 1. 查询当前用户的分组列表，按 sortOrder 降序，更新时间降序排列
        LambdaQueryWrapper<GroupDO> queryWrapper = Wrappers.lambdaQuery(GroupDO.class)
                .eq(GroupDO::getDelFlag, 0) // 未删除
                .eq(GroupDO::getUsername, UserContext.getUsername())
                .orderByDesc(GroupDO::getSortOrder, GroupDO::getUpdateTime);
        List<GroupDO> groupDOList = baseMapper.selectList(queryWrapper);

        // 2. [亮点]：调用远程服务获取每个分组下的短链接数量。
        // 这里提取出所有的 gid 列表，进行批量查询，避免循环调用（N+1问题）。
        Result<List<ShortLinkGroupCountQueryRespDTO>> listResult = shortLinkRemoteService
                .listGroupShortLinkCount(groupDOList.stream().map(GroupDO::getGid).toList());

        // 3. 将数据库实体 GroupDO 转换为响应对象 DTO
        List<ShortLinkGroupRespDTO> shortLinkGroupRespDTOList = BeanUtil.copyToList(groupDOList, ShortLinkGroupRespDTO.class);

        // 4. [亮点]：内存数据聚合。
        // 将远程查到的“数量数据”填充到“分组列表数据”中。
        // 这种在应用层进行 join 的操作是微服务架构中常见的模式。
        shortLinkGroupRespDTOList.forEach(each -> {
            Optional<ShortLinkGroupCountQueryRespDTO> first = listResult.getData().stream()
                    .filter(item -> Objects.equals(item.getGid(), each.getGid()))
                    .findFirst();
            // 如果找到了对应的计数对象，则设置数量；否则默认为 0（DTO中Integer默认为null，这里需要注意判空）
            first.ifPresent(item -> each.setShortLinkCount(first.get().getShortLinkCount()));
        });
        return shortLinkGroupRespDTOList;
    }

    /**
     * 修改短链接分组名称
     */
    @Override
    public void updateGroup(ShortLinkGroupUpdateReqDTO requestParam) {
        // 构造更新条件：当前用户 + 指定GID + 未删除
        LambdaUpdateWrapper<GroupDO> updateWrapper = Wrappers.lambdaUpdate(GroupDO.class)
                .eq(GroupDO::getUsername, UserContext.getUsername())
                .eq(GroupDO::getGid, requestParam.getGid())
                .eq(GroupDO::getDelFlag, 0);
        GroupDO groupDO = new GroupDO();
        groupDO.setName(requestParam.getName());
        baseMapper.update(groupDO, updateWrapper);
    }

    /**
     * 删除短链接分组
     * [亮点]：软删除（Soft Delete）。
     * 并没有真正从数据库移除记录，而是将 del_flag 标记为 1。
     */
    @Override
    public void deleteGroup(String gid) {
        LambdaUpdateWrapper<GroupDO> updateWrapper = Wrappers.lambdaUpdate(GroupDO.class)
                .eq(GroupDO::getUsername, UserContext.getUsername())
                .eq(GroupDO::getGid, gid)
                .eq(GroupDO::getDelFlag, 0);
        GroupDO groupDO = new GroupDO();
        groupDO.setDelFlag(1); // 标记删除
        baseMapper.update(groupDO, updateWrapper);
    }

    /**
     * 短链接分组排序
     */
    @Override
    public void sortGroup(List<ShortLinkGroupSortReqDTO> requestParam) {
        // TODO [潜在优化点]：这里在循环中执行 SQL 更新。
        // 如果分组数量较多，会对数据库造成一定压力。可以考虑 SQL Case When 语法进行批量更新。
        requestParam.forEach(each -> {
            GroupDO groupDO = GroupDO.builder()
                    .sortOrder(each.getSortOrder())
                    .build();
            LambdaUpdateWrapper<GroupDO> updateWrapper = Wrappers.lambdaUpdate(GroupDO.class)
                    .eq(GroupDO::getUsername, UserContext.getUsername())
                    .eq(GroupDO::getGid, each.getGid())
                    .eq(GroupDO::getDelFlag, 0);
            baseMapper.update(groupDO, updateWrapper);
        });
    }

    /**
     * 检查 GID 是否已存在
     * * @param username 用户名
     * @param gid 分组标识
     * @return true: 不存在(可用); false: 已存在(不可用)
     */
    private boolean hasGid(String username, String gid) {
        LambdaQueryWrapper<GroupDO> queryWrapper = Wrappers.lambdaQuery(GroupDO.class)
                .eq(GroupDO::getGid, gid)
                // 如果传入的 username 为空，则使用当前上下文的用户
                .eq(GroupDO::getUsername, Optional.ofNullable(username).orElse(UserContext.getUsername()));
        GroupDO hasGroupFlag = baseMapper.selectOne(queryWrapper);
        return hasGroupFlag == null;
    }
}