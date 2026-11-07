package cn.com.agentflow.quota;

/**
 * 配额预扣 Saga 状态机：
 * INIT（已登记、尚未预扣）→ RESERVED（预扣成功）→ SETTLED（正常结算）/ REFUNDED（对账退款）。
 * 终态 SETTLED、REFUNDED 不再被对账任务扫描。
 */
public enum QuotaReservationStatus {
    INIT,
    RESERVED,
    SETTLED,
    REFUNDED
}
