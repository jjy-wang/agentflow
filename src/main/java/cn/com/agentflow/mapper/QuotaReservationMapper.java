package cn.com.agentflow.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectKey;
import org.apache.ibatis.annotations.Update;

import java.util.List;

/**
 * t_quota_reservation 账本 Mapper。
 * 状态迁移全部用条件更新（CAS），多实例下只有一个执行者能成功。
 */
@Mapper
public interface QuotaReservationMapper {

    /** 登记 INIT：此时尚未预扣，reserved_tokens 保持 0；主键由 BIGSERIAL 序列生成 */
    @Insert("INSERT INTO t_quota_reservation(run_id, model, tpm_limit, status) "
            + "VALUES (#{runId}, #{model}, #{tpmLimit}, 'INIT')")
    @SelectKey(before = true, keyProperty = "id", resultType = long.class,
            statement = "SELECT nextval('t_quota_reservation_id_seq')")
    int insertInit(QuotaReservationRow row);

    /** 预扣成功：INIT → RESERVED，并记录实际预扣量 */
    @Update("UPDATE t_quota_reservation SET status = 'RESERVED', reserved_tokens = #{reservedTokens}, "
            + "updated_at = now() WHERE id = #{id} AND status = 'INIT'")
    int casReserved(@Param("id") long id, @Param("reservedTokens") int reservedTokens);

    /** 正常结算完成：RESERVED → SETTLED */
    @Update("UPDATE t_quota_reservation SET status = 'SETTLED', actual_tokens = #{actualTokens}, "
            + "updated_at = now() WHERE id = #{id} AND status = 'RESERVED'")
    int casSettled(@Param("id") long id, @Param("actualTokens") int actualTokens);

    /**
     * 对账扫描：只捞非终态且超过 stale-after 的账本。
     * created_at 与 updated_at 同时判陈旧，前者走 (status, created_at) 索引。
     */
    @Select("SELECT id, run_id, model, tpm_limit, reserved_tokens, actual_tokens, status "
            + "FROM t_quota_reservation "
            + "WHERE status IN ('INIT', 'RESERVED') "
            + "AND created_at < now() - (#{staleSeconds} || ' second')::interval "
            + "AND updated_at < now() - (#{staleSeconds} || ' second')::interval "
            + "ORDER BY created_at LIMIT #{batch}")
    @Results(id = "quotaReservationRowMap", value = {
            @Result(column = "id", property = "id"),
            @Result(column = "run_id", property = "runId"),
            @Result(column = "model", property = "model"),
            @Result(column = "tpm_limit", property = "tpmLimit"),
            @Result(column = "reserved_tokens", property = "reservedTokens"),
            @Result(column = "actual_tokens", property = "actualTokens"),
            @Result(column = "status", property = "status")
    })
    List<QuotaReservationRow> findStale(@Param("staleSeconds") long staleSeconds,
                                        @Param("batch") int batch);

    /**
     * 抢占退款权：只有把状态从 INIT/RESERVED CAS 成 REFUNDED 成功的实例才继续执行退款，
     * 抢不到（返回 0）直接跳过，保证重复执行/多实例不会重复退款。
     */
    @Update("UPDATE t_quota_reservation SET status = 'REFUNDED', updated_at = now() "
            + "WHERE id = #{id} AND status = #{expectedStatus}")
    int claimRefund(@Param("id") long id, @Param("expectedStatus") String expectedStatus);
}
