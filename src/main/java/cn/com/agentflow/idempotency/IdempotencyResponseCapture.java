package cn.com.agentflow.idempotency;

import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 响应体旁路捕获：只读不改写，字节仍原样写给客户端，同时攒一份用于幂等重放。
 * 仅用于 POST /v1/agent/runs 这类小 JSON 响应；SSE 走 writeAndFlushWith，不在此缓存。
 * 超过上限时放弃缓存响应体，但仍保留"已完成"状态，避免同 key 重复创建。
 */
class IdempotencyResponseCapture extends ServerHttpResponseDecorator {

    private final int maxBytes;
    private final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    private boolean overflow;

    IdempotencyResponseCapture(ServerHttpResponse delegate, int maxBytes) {
        super(delegate);
        this.maxBytes = maxBytes;
    }

    @Override
    public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
        return super.writeWith(Flux.from(body).doOnNext(buffer -> {
            if (overflow) {
                return;
            }
            int readable = buffer.readableByteCount();
            if (captured.size() + readable > maxBytes) {
                overflow = true;
                captured.reset();
                return;
            }
            byte[] bytes = new byte[readable];
            // toByteBuffer 返回副本，不会移动原始 DataBuffer 的读指针
            buffer.toByteBuffer().get(bytes);
            captured.write(bytes, 0, bytes.length);
        }));
    }

    /** 超限返回 null，调用方按"不可缓存"处理 */
    String body() {
        return overflow ? null : captured.toString(StandardCharsets.UTF_8);
    }
}
