package cn.com.agentflow.quota;

public record TokenReservation(String model, int reserved, boolean granted, String reason) {
    public static TokenReservation deny(String model, String reason) {
        return new TokenReservation(model, 0, false, reason);
    }
}
