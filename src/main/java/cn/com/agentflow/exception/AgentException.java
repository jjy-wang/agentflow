package cn.com.agentflow.exception;

import lombok.Getter;

@Getter
public class AgentException extends RuntimeException {

    private final String code;

    public AgentException(String code, String message) {
        super(message);
        this.code = code;
    }
}
