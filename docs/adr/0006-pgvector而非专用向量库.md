# ADR 0006：pgvector 而非专用向量库

- 状态：已采纳
- 日期：2026-10

## 背景

长期记忆需要向量召回：把历史事实 embedding 后按余弦相似度召回 Top-K 注入上下文。选型在 pgvector（PostgreSQL 扩展）与专用向量库（Milvus、Qdrant、Weaviate）之间。本项目的记忆规模在百万向量级以下，且记忆写入需要与业务数据（session、message）保持事务一致。

## 决策

采用 pgvector（`pgvector/pgvector:pg16` 镜像，HNSW 索引）承载长期记忆：

- 向量与业务表同库，`VectorMemory` 写入与消息落库同事务；
- 召回 = 一次 SQL（余弦距离 + 阈值 0.75 + Top-K 5 + 时效衰减加权），走只读数据源；
- 不新增任何中间件，备份、监控、迁移复用现有 PG 体系（Flyway `V2__pgvector.sql`）。

## 备选方案

1. **Milvus**：被否。召回性能与规模上限更强，但要独立部署一套集群（含 etcd、对象存储依赖），运维成本与团队规模不匹配；且向量与业务数据分离后，一致性要靠最终一致补偿。
2. **Qdrant / Weaviate**：被否。理由同上，规模未到需要专用库的程度。
3. **Redis Stack 向量检索**：被否。Redis 在本项目中是缓存/限流/回放角色（allkeys-lru 可驱逐），把需要持久、可关联查询的记忆放进去语义错误。
4. **Elasticsearch 向量检索**：被否。技术栈明确禁止引入 ES。

## 代价

- **召回性能弱于 Milvus**：HNSW 在 pgvector 上的 QPS 与召回率调优空间小于专用库；当前规模（百万级、Top-K=5、阈值 0.75）实测不是瓶颈，但量级上到千万后需要重新评估；
- **大规模需分片**：单表向量膨胀后索引构建与查询都会变慢，届时要按租户或时间分片，甚至迁往专用向量库；
- **与业务库争资源**：向量检索是 CPU 密集操作，高负载下与业务 SQL 争抢同一 PG 实例的 CPU 与 IO，需要靠只读从库（ro 数据源）卸载查询压力。

## 验证

- `V2__pgvector.sql` 迁移在 Flyway 流程内自动建扩展与 HNSW 索引，全栈 `docker compose up -d` 即可用；
- 召回链路压测：记忆查询走只读数据源（ro-pool），主库写入不受向量检索 CPU 开销影响；
- 慢查询监控（`log_min_duration_statement=200` + `pg_stat_statements`）下，Top-K 5、阈值 0.75 的召回 SQL 未进入慢查询名单。

## 结论

数据量 < 千万、要求事务一致、不想多一套中间件——三条都成立时 pgvector 是正确默认。代价（召回性能上限、未来分片）被明确接受，迁移路径（`VectorMemory` 接口隔离，可整体替换实现）已在代码层预留。
