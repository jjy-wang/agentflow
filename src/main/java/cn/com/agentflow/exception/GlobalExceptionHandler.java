package cn.com.agentflow.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(AgentException.class)
    public ResponseEntity<Map<String, Object>> handle(AgentException e) {
        int http = switch (e.getCode()) {
            case "UNAUTHORIZED", "RUN_NOT_FOUND" -> 401;
            case ErrorCodes.RATE_LIMITED, ErrorCodes.QUOTA_EXCEEDED,
                 ErrorCodes.MODEL_BUSY, "RPM_EXCEEDED", "TPM_EXCEEDED" -> 429;
            case "SSE_CONN_LIMIT" -> 429;
            // 候选模型都调过但上游失败：网关侧临时故障，不是请求方的错
            case ErrorCodes.UPSTREAM_ERROR -> 502;
            // 路由真的没有可用模型：服务端能力缺失
            case ErrorCodes.NO_MODEL_AVAILABLE -> 503;
            default -> 500;
        };
        return ResponseEntity.status(HttpStatus.valueOf(http))
                .body(Map.of("code", jsonRpcCode(e.getCode()), "message", e.getMessage()));
    }

    /** 协议层与业务层错误码分开： -320xx 是网关业务错误 */
    private int jsonRpcCode(String code) {
        return switch (code) {
            case "UNAUTHORIZED" -> -32001;
            case "RUN_NOT_FOUND" -> -32002;
            case "TOOL_NOT_FOUND" -> -32003;
            case ErrorCodes.NO_MODEL_AVAILABLE -> -32004;
            case "RPM_EXCEEDED", "TPM_EXCEEDED",
                 ErrorCodes.QUOTA_EXCEEDED, ErrorCodes.RATE_LIMITED -> -32005;
            case ErrorCodes.MODEL_BUSY -> -32006;
            case "TTFT_TIMEOUT" -> -32007;
            case ErrorCodes.UPSTREAM_ERROR -> -32008;
            default -> -32603;
        };
    }
}
