-- 1. 接收参数
-- KEYS[1]: 这里的参数由 Java 端的 Lists.newArrayList(username) 传入
-- 代表要限流的目标对象（用户名）
local username = KEYS[1]

-- ARGV[1]: 这里的参数由 Java 端的 userFlowRiskControlConfiguration.getTimeWindow() 传入
-- 代表限流的时间窗口大小（例如：1秒、10秒）
local timeWindow = tonumber(ARGV[1]) -- 时间窗口，单位：秒

-- 2. 构造 Redis Key
-- 最终在 Redis 中生成的 Key 类似于：short-link:user-flow-risk-control:zhangsan
local accessKey = "short-link:user-flow-risk-control:" .. username

-- 3. 执行核心计数逻辑 (INCR)
-- redis.call 是在 Lua 中调用 Redis 命令的标准方式
-- INCR 命令：将 Key 中储存的数字值增一。
-- 如果 Key 不存在，那么 Key 的值会先被初始化为 0 ，然后再执行 INCR 操作（变为 1）。
-- 返回值：执行 INCR 命令之后 Key 的值。
local currentAccessCount = redis.call("INCR", accessKey)

-- 4. 设置/重置过期时间 (EXPIRE)
-- 【关键点】：这里每次请求都会重置过期时间！
-- 也就是说，只要用户持续访问，这个 Key 就永远不会过期，计数器也不会清零。
-- 用户必须完全停止访问，等待 timeWindow 秒后，Key 才会自动删除，计数器才会重置。
-- 这是一种“惩罚性”的风控策略，要求用户必须有“冷却期”。
redis.call("EXPIRE", accessKey, timeWindow)

-- 5. 返回结果
-- 将当前的访问次数返回给 Java 代码。
-- Java 代码拿到这个值后，会跟配置的 maxAccessCount 进行比对，决定是否拦截。
return currentAccessCount