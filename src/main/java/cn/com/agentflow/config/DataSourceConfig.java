package cn.com.agentflow.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * 读写分离数据源：
 * - rw：主库，承载全部写与默认读（Flyway 迁移也走它，路由默认键即 RW）
 * - ro：只读库（本地用同一 PG 实例 + 只读账号模拟；生产指向流复制从库）
 * 路由规则为显式路由：@Transactional(readOnly=true) 的事务走 RO，否则走 RW。
 * 判定基于 Spring 事务同步上下文，不引入任何自定义 ThreadLocal。
 */
@Configuration
public class DataSourceConfig {

    private static final String RW = "rw";
    private static final String RO = "ro";

    @Bean
    @ConfigurationProperties("spring.datasource.rw")
    public DataSourceProperties rwDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.ro")
    public DataSourceProperties roDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.rw.hikari")
    public HikariDataSource rwDataSource(
            @Qualifier("rwDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.ro.hikari")
    public HikariDataSource roDataSource(
            @Qualifier("roDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    /** MyBatis-Plus / JdbcTemplate 默认使用的主数据源：无事务或读写事务都落 RW */
    @Bean
    @Primary
    public DataSource dataSource(@Qualifier("rwDataSource") DataSource rw,
                                 @Qualifier("roDataSource") DataSource ro) {
        Map<Object, Object> targets = new HashMap<>();
        targets.put(RW, rw);
        targets.put(RO, ro);

        AbstractRoutingDataSource routing = new AbstractRoutingDataSource() {
            @Override
            protected Object determineCurrentLookupKey() {
                // 显式路由：只读事务 → 从库；无事务（含 Flyway、普通 JdbcTemplate 调用）→ 主库
                boolean readOnlyTx = TransactionSynchronizationManager.isCurrentTransactionReadOnly()
                        && TransactionSynchronizationManager.isActualTransactionActive();
                return readOnlyTx ? RO : RW;
            }
        };
        routing.setTargetDataSources(targets);
        routing.setDefaultTargetDataSource(rw);
        return routing;
    }
}
