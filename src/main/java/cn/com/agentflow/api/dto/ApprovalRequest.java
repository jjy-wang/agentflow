package cn.com.agentflow.api.dto;

import jakarta.validation.constraints.Size;
import lombok.Data;

/** HITL 审批回调入参 */
@Data
public class ApprovalRequest {

    /** 审批人标识 */
    @Size(max = 64, message = "approver 过长")
    private String approver;

    /** 审批意见 */
    @Size(max = 512, message = "comment 过长")
    private String comment;
}
