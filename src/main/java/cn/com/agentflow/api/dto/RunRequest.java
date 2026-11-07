package cn.com.agentflow.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
public class RunRequest {
    @NotBlank(message = "query 不能为空")
    @Size(max = 8000, message = "query 过长")
    private String query;

    private Long sessionId;

    private String model;

    private Integer maxSteps;
}
