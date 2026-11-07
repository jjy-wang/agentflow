package cn.com.agentflow.tool;

import cn.com.agentflow.agent.RunContext;
import cn.com.agentflow.llm.ToolDef;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 工具注册中心：启动扫描 @Tool 方法，按租户授权过滤后暴露给模型
 */
@Slf4j
@Component
public class ToolRegistry implements SmartInitializingSingleton {

    private final Map<String, ToolMeta> tools = new ConcurrentHashMap<>();
    private final ApplicationContext ctx;
    private final ObjectMapper mapper;
    private final JdbcTemplate jdbc;
    /** 授权缓存：key 为 tenantId，value 为该租户已授权且已注册的工具名集合；空结果同样缓存防穿透 */
    private final Cache<String, Set<String>> grantCache;

    public ToolRegistry(ApplicationContext ctx, ObjectMapper mapper, JdbcTemplate jdbc) {
        this.ctx = ctx;
        this.mapper = mapper;
        this.jdbc = jdbc;
        this.grantCache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(60))
                .maximumSize(10_000)
                .build();
    }

    /**
     * 推迟到所有单例创建完成后再扫描。
     * 构造期扫描会触发 ApplicationContext 实例化全部 @Component，与 AgentRuntime→ReActLoop→ToolRegistry
     * 形成构造器循环依赖，导致应用无法启动。
     */
    @Override
    public void afterSingletonsInstantiated() {
        scan(ctx);
    }

    private void scan(ApplicationContext ctx) {
        Map<String, Object> beans = ctx.getBeansWithAnnotation(Component.class);
        beans.values().forEach(bean -> {
            Class<?> clazz = AopUtils.getTargetClass(bean);
            for (Method m : clazz.getDeclaredMethods()) {
                Tool tool = m.getAnnotation(Tool.class);
                if (tool == null) {
                    continue;
                }
                validate(tool, m);
                m.setAccessible(true);
                ToolHandler handler = (args, toolCtx) -> {
                    Object[] params = bind(m, args, toolCtx);
                    Object r = m.invoke(bean, params);
                    return (ToolResult) r;
                };
                JsonNode schema = new SchemaGenerator(mapper).generate(m);
                tools.put(tool.name(), new ToolMeta(tool.name(), tool.description(),
                        schema, tool.riskLevel(), tool.timeoutMs(), handler));
                log.info("注册工具: {} risk={} params={}", tool.name(), tool.riskLevel(),
                        schema.path("properties").size());
            }
        });
        if (tools.isEmpty()) {
            throw new IllegalStateException("未扫描到任何 @Tool 方法");
        }
    }

    /** 启动期校验：描述缺失、名称非法、参数含 tenantId 一律启动失败 */
    private void validate(Tool tool, Method m) {
        if (!tool.name().matches("^[a-z][a-z0-9_]{2,63}$")) {
            throw new IllegalStateException("工具名不合法: " + tool.name());
        }
        if (tool.description() == null || tool.description().isBlank()) {
            throw new IllegalStateException("工具缺少描述: " + tool.name());
        }
        for (var p : m.getParameters()) {
            ToolParam tp = p.getAnnotation(ToolParam.class);
            if (tp == null) {
                // RunContext 由框架注入，不作为模型可见参数
                if (p.getType() == RunContext.class) {
                    continue;
                }
                throw new IllegalStateException("参数缺少 @ToolParam: " + tool.name());
            }
            if (tp.description() == null || tp.description().isBlank()) {
                throw new IllegalStateException("参数缺少描述: " + tool.name() + "." + tp.name());
            }
            if ("tenantId".equalsIgnoreCase(tp.name())) {
                throw new IllegalStateException("工具参数禁止出现 tenantId（防模型伪造租户）: " + tool.name());
            }
        }
    }

    private Object[] bind(Method m, JsonNode args, RunContext ctx) {
        var params = m.getParameters();
        Object[] out = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            if (params[i].getType() == RunContext.class) {
                out[i] = ctx;
                continue;
            }
            ToolParam tp = params[i].getAnnotation(ToolParam.class);
            JsonNode v = args.path(tp.name());
            Class<?> t = params[i].getType();
            if (v.isMissingNode() || v.isNull()) {
                out[i] = tp.required() ? defaultValue(t) : null;
            } else if (t == String.class) {
                out[i] = v.asText();
            } else if (t == Integer.class || t == int.class) {
                out[i] = v.asInt();
            } else if (t == Long.class || t == long.class) {
                out[i] = v.asLong();
            } else if (t == Boolean.class || t == boolean.class) {
                out[i] = v.asBoolean();
            } else {
                out[i] = mapper.convertValue(v, t);
            }
        }
        return out;
    }

    private Object defaultValue(Class<?> t) {
        if (t == int.class) return 0;
        if (t == long.class) return 0L;
        if (t == boolean.class) return false;
        return null;
    }

    public Optional<ToolMeta> find(String name) {
        return Optional.ofNullable(tools.get(name));
    }

    /** 对外暴露给模型的工具定义（只含已授权工具） */
    public List<ToolDef> definitions(String tenantId) {
        Set<String> granted = grantedTools(tenantId);
        List<ToolDef> list = new ArrayList<>();
        tools.forEach((name, meta) -> {
            if (granted.contains(name)) {
                list.add(new ToolDef(meta.name(), meta.description(), meta.schema()));
            }
        });
        return list;
    }

    /**
     * 查询租户已授权工具：真实查库 t_tool_grant（授权维度 tenant_id + enabled），
     * Caffeine 缓存 60s；未授权任何工具时缓存空集合防止缓存穿透。
     */
    public Set<String> grantedTools(String tenantId) {
        Set<String> granted = grantCache.get(tenantId, this::loadGrantedTools);
        return granted.stream()
                .filter(tools::containsKey)
                .collect(Collectors.toCollection(HashSet::new));
    }

    /** 缓存加载器：阻塞 JDBC，仅缓存未命中时执行一次，60s 内命中缓存 */
    private Set<String> loadGrantedTools(String tenantId) {
        List<String> names = jdbc.queryForList(
                "SELECT tool_name FROM t_tool_grant WHERE tenant_id = ?::bigint AND enabled = 1",
                String.class, tenantId);
        return Set.copyOf(names);
    }

    public void invalidateGrant(String tenantId) {
        grantCache.invalidate(tenantId);
    }
}
