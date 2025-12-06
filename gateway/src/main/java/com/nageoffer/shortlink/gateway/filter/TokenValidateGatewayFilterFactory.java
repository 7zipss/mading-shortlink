package com.nageoffer.shortlink.gateway.filter;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.nageoffer.shortlink.gateway.config.Config;
import com.nageoffer.shortlink.gateway.dto.GatewayErrorResult;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * SpringCloud Gateway Token 拦截器
 * 继承 AbstractGatewayFilterFactory 是自定义网关过滤器的标准写法
 * <Config> 指向的是同包下的 Config 类，用于接收 yaml 文件中的配置参数（如白名单）
 */
@Component
public class TokenValidateGatewayFilterFactory extends AbstractGatewayFilterFactory<Config> {

    private final StringRedisTemplate stringRedisTemplate;
    /**
     * config注入过程：
     *
     * Gateway 启动时扫描到 TokenValidateGatewayFilterFactory。
     *
     * 它发现这个工厂继承了 AbstractGatewayFilterFactory<Config>，于是知道配置类是 Config.class。
     *
     * 当解析路由配置时，看到 args 下有个 whitePathList。
     *
     * Gateway 利用反射调用 Config 类的 setWhitePathList() 方法（Lombok 生成的），把 YAML 里的列表数据注入进去。
     *
     * 最后，调用 apply(config) 方法时，这个 config 对象里就已经有值了。
     */
    // 构造函数注入 Redis 客户端，用于查 Token
    public TokenValidateGatewayFilterFactory(StringRedisTemplate stringRedisTemplate) {
        super(Config.class);
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 核心过滤逻辑
     * @param config 从 application.yaml 中读取到的配置对象（包含白名单列表）
     * @return GatewayFilter 过滤器链
     * 【apply 方法的返回值类型是 GatewayFilter。而 GatewayFilter 是一个函数式接口，它只有一个抽象方法：
     * 所以利用了 Java 8 的 Lambda 表达式 和 函数式接口 特性，直接return一个 GatewayFilter 对象。】
     */
    @Override
    public GatewayFilter apply(Config config) {
        //exchange：当前的请求上下文（包含 Request、Response 等）。
        //chain：过滤器链（用于把请求传给下一个过滤器）。
        return (exchange, chain) -> {
            // 1. 获取当前请求对象
            ServerHttpRequest request = exchange.getRequest();
            String requestPath = request.getPath().toString();
            String requestMethod = request.getMethod().name();

            // 2. 判断路径是否在白名单中（例如 /login, /register）
            // 如果不在白名单，才需要进行 Token 校验
            if (!isPathInWhiteList(requestPath, requestMethod, config.getWhitePathList())) {

                // 3. 从请求头获取 username 和 token
                String username = request.getHeaders().getFirst("username");
                String token = request.getHeaders().getFirst("token");
                Object userInfo;

                // 4. 去 Redis 校验 Token 是否有效
                // 校验规则：username 和 token 非空，且 Redis 中存在对应的 Key
                // Redis Key 结构: "short-link:login:{username}"
                if (StringUtils.hasText(username) && StringUtils.hasText(token) && (userInfo = stringRedisTemplate.opsForHash().get("short-link:login:" + username, token)) != null) {

                    // --- 校验通过 ---

                    // 5. 解析 Redis 中的用户信息
                    JSONObject userInfoJsonObject = JSON.parseObject(userInfo.toString());

                    // 6. 【关键步骤】请求头透传 (Header Mutation)
                    // TODO 在转发给下游微服务之前，修改请求头，把 userId 和 realName 塞进去。 【admin的filter也修改为验证userId和realName了】
                    // 这样，下游的 Admin/Project 服务就不需要再查 Redis，直接从 Header 取 userId 即可。
                    ServerHttpRequest.Builder builder = exchange.getRequest().mutate().headers(httpHeaders -> {
                        httpHeaders.set("userId", userInfoJsonObject.getString("id"));
                        // 中文需要 URL 编码，防止 Header乱码
                        httpHeaders.set("realName", URLEncoder.encode(userInfoJsonObject.getString("realName"), StandardCharsets.UTF_8));
                    });

                    // 7. 放行，并使用修改后的请求继续调用链
                    return chain.filter(exchange.mutate().request(builder.build()).build());
                }

                // --- 校验失败 ---

                // 8. 设置 401 未授权状态码
                ServerHttpResponse response = exchange.getResponse();
                response.setStatusCode(HttpStatus.UNAUTHORIZED);

                // 9. 返回 JSON 格式的错误提示
                return response.writeWith(Mono.fromSupplier(() -> {
                    DataBufferFactory bufferFactory = response.bufferFactory();
                    GatewayErrorResult resultMessage = GatewayErrorResult.builder()
                            .status(HttpStatus.UNAUTHORIZED.value())
                            .message("Token validation error") // Token 校验失败
                            .build();
                    return bufferFactory.wrap(JSON.toJSONString(resultMessage).getBytes());
                }));
            }

            // 10. 如果在白名单中，直接放行，不做任何处理
            return chain.filter(exchange);
        };
    }

    /**
     * 判断路径是否在白名单中
     * @param requestPath 当前请求路径
     * @param requestMethod 当前请求方法
     * @param whitePathList yaml配置的白名单列表
     */
    private boolean isPathInWhiteList(String requestPath, String requestMethod, List<String> whitePathList) {
        // 规则1：如果路径以白名单列表中的任意一个字符串开头（startsWith），则通过
        // 规则2：特别硬编码放行用户注册接口 (POST /api/short-link/admin/v1/user)
        return (!CollectionUtils.isEmpty(whitePathList) && whitePathList.stream().anyMatch(requestPath::startsWith))
                || (Objects.equals(requestPath, "/api/short-link/admin/v1/user") && Objects.equals(requestMethod, "POST"));
    }
}