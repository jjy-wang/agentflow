package cn.com.agentflow.consumer;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 报表任务状态存取。表名以 V5__report_task.sql 为准：report_task（无 t_ 前缀）。
 * 状态流转用带前置状态的条件 UPDATE 实现 CAS，多实例/重复投递下只有一个消费者能认领。
 */
@Component
@RequiredArgsConstructor
public class ReportTaskStore {

    public static final String PENDING = "PENDING";
    public static final String RUNNING = "RUNNING";
    public static final String SUCCEEDED = "SUCCEEDED";
    public static final String FAILED = "FAILED";

    private final JdbcTemplate jdbc;

    /** PENDING → RUNNING，返回 true 表示当前消费者抢到该任务 */
    public boolean claim(String taskId) {
        int rows = jdbc.update(
                "UPDATE report_task SET status = 'RUNNING' WHERE task_id = ?::uuid AND status = 'PENDING'",
                taskId);
        return rows == 1;
    }

    /** 查不到记录返回 null */
    public String findStatus(String taskId) {
        List<String> rows = jdbc.queryForList(
                "SELECT status FROM report_task WHERE task_id = ?::uuid", String.class, taskId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** RUNNING → SUCCEEDED */
    public void markSucceeded(String taskId) {
        jdbc.update("UPDATE report_task SET status = 'SUCCEEDED' WHERE task_id = ?::uuid AND status = 'RUNNING'",
                taskId);
    }

    /** PENDING/RUNNING → FAILED，终态任务不会被重复标记 */
    public void markFailed(String taskId) {
        jdbc.update("UPDATE report_task SET status = 'FAILED' WHERE task_id = ?::uuid AND status IN ('PENDING','RUNNING')",
                taskId);
    }
}
