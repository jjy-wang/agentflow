package cn.com.agentflow.mapper;

import lombok.Data;

/**
 * 配额预扣账本行（t_quota_reservation）。
 * 仅用于 Saga 对账，不参与在线限流判定。
 */
@Data
public class QuotaReservationRow {

    private Long id;

    /** 关联的 run；调用方未透传时退化为每次 LLM 调用的关联 ID */
    private String runId;

    private String model;

    /** 预扣时使用的桶容量，保证退款不依赖当前配置 */
    private Integer tpmLimit;

    /** RESERVED 时记录实际预扣量；INIT 阶段为 0 */
    private Integer reservedTokens;

    /** 实际消耗，SETTLED 时写入 */
    private Integer actualTokens;

    /** INIT / RESERVED / SETTLED / REFUNDED */
    private String status;
}
