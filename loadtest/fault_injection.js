import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// 故障注入压测：通过 mock-llm 的 X-Mock-* 请求头注入 6 类上游故障，
// 验证 AgentFlow 的降级 / 重试 / 舱壁 / 快速失败行为是否符合预期。
// 每类故障一个 scenario，独立指标与阈值，互不干扰。
// 前置：全栈已启动（docker compose up -d），X-Mock-* 头需随请求透传至 mock-llm
// （或在网关侧按路由配置注流）；只打 mock-llm，严禁打真实模型。

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const API_KEY = __ENV.API_KEY || 'demo-key-123456';

// 各场景独立指标
const highLatencyDuration = new Trend('fi_high_latency_duration');
const rateLimitedOk = new Rate('fi_rate_limit_handled');
const fiveXxHandled = new Rate('fi_5xx_handled');
const slowTtftFastFail = new Trend('fi_slow_ttft_duration');
const truncationOk = new Rate('fi_truncation_handled');
const connResetHandled = new Rate('fi_conn_reset_handled');
const connResetDuration = new Trend('fi_conn_reset_duration');
const bulkheadRejected = new Counter('fi_bulkhead_rejected');

export const options = {
  scenarios: {
    // 1) 高延迟：上游 3s 延迟，系统应慢而不挂
    high_latency: {
      executor: 'constant-vus',
      vus: 10,
      duration: '30s',
      exec: 'highLatency',
      startTime: '0s',
    },
    // 2) 限流 429：突发超发，配额预检应返回 429 而非 5xx
    rate_limit_429: {
      executor: 'constant-arrival-rate',
      rate: 300,
      timeUnit: '1s',
      duration: '20s',
      preAllocatedVUs: 100,
      maxVUs: 400,
      exec: 'rateLimit',
      startTime: '0s',
    },
    // 3) 5xx 突发：上游 50% 失败率，路由重试/降级后错误率受控
    burst_5xx: {
      executor: 'constant-vus',
      vus: 20,
      duration: '30s',
      exec: 'burst5xx',
      startTime: '35s',
    },
    // 4) 慢 TTFT：首包迟迟不来，TTFT 超时保护必须快速失败
    slow_ttft: {
      executor: 'constant-vus',
      vus: 5,
      duration: '20s',
      exec: 'slowTtft',
      startTime: '35s',
    },
    // 5) token 截断：超长输出被 maxTokens 截断，聚合不应卡死
    token_truncation: {
      executor: 'constant-vus',
      vus: 10,
      duration: '30s',
      exec: 'tokenTruncation',
      startTime: '70s',
    },
    // 6) 连接重置：上游吐一半断开，系统须收尾（失败事件或受控错误），不悬挂
    connection_reset: {
      executor: 'constant-vus',
      vus: 10,
      duration: '30s',
      exec: 'connectionReset',
      startTime: '70s',
    },
  },
  thresholds: {
    // 高延迟：P95 允许放大到 8s，但不能报错
    fi_high_latency_duration: ['p(95)<8000'],
    // 限流：200 或 429 都算"被正确处理"，不允许 5xx 外溢
    fi_rate_limit_handled: ['rate>0.99'],
    // 5xx 突发：受控响应（200/502/504）比例 > 95%
    fi_5xx_handled: ['rate>0.95'],
    // 慢 TTFT：快速失败，P95 必须明显小于上游 120s 悬挂
    fi_slow_ttft_duration: ['p(95)<30000'],
    // 截断：run 创建全部受理
    fi_truncation_handled: ['rate>0.99'],
    // 连接重置：全部在 30s 内收尾
    fi_conn_reset_handled: ['rate>0.95'],
    fi_conn_reset_duration: ['p(95)<30000'],
  },
};

function postRun(body, extraHeaders, timeout) {
  return http.post(`${BASE}/v1/agent/runs`, JSON.stringify(body), {
    headers: Object.assign({
      'Content-Type': 'application/json',
      'X-Api-Key': API_KEY,
    }, extraHeaders || {}),
    timeout: timeout || '60s',
  });
}

// 1) 高延迟：注入 3s 首包延迟
export function highLatency() {
  const start = Date.now();
  const res = postRun(
    { query: '查询 10.0.0.1 主机', model: 'mock-large' },
    { 'X-Mock-Delay-Ms': '3000' });
  highLatencyDuration.add(Date.now() - start);
  check(res, { '高延迟下仍受理': (r) => r.status === 200 });
}

// 2) 限流 429：超发触发配额，429 属于正确降级
export function rateLimit() {
  const res = postRun({ query: '查询资产', model: 'mock-large' });
  if (res.status === 429) {
    bulkheadRejected.add(1);
  }
  rateLimitedOk.add(res.status === 200 || res.status === 429);
  check(res, { '限流受控（200/429）': (r) => r.status === 200 || r.status === 429 });
}

// 3) 5xx 突发：上游一半概率直接 500
export function burst5xx() {
  const res = postRun(
    { query: '查询资产', model: 'mock-large' },
    { 'X-Mock-Fail-Rate': '0.5', 'X-Mock-Fail-Mode': 'error' });
  // 200 = 重试到其他候选模型成功；502/504 = 受控失败事件。不允许连接被挂死或无响应
  fiveXxHandled.add(res.status === 200 || res.status === 502 || res.status === 504);
  check(res, { '5xx 受控': (r) => [200, 502, 504].includes(r.status) });
}

// 4) 慢 TTFT：上游 120s 不出首包，验证 TTFT 快速失败
export function slowTtft() {
  const start = Date.now();
  const res = postRun(
    { query: '查询资产', model: 'mock-large' },
    { 'X-Mock-Fail-Rate': '1', 'X-Mock-Fail-Mode': 'timeout' },
    '40s');
  slowTtftFastFail.add(Date.now() - start);
  check(res, { 'TTFT 超时快速收尾': (r) => [200, 502, 504].includes(r.status) });
}

// 5) token 截断：上游猛吐 4000 token，服务端按预算截断
export function tokenTruncation() {
  const res = postRun(
    { query: '生成一份很长的报告', model: 'mock-large' },
    { 'X-Mock-Tokens': '4000', 'X-Mock-Tps': '1000' },
    '60s');
  truncationOk.add(res.status === 200);
  check(res, { '截断下 run 正常受理': (r) => r.status === 200 });
}

// 6) 连接重置：上游吐一半断开
export function connectionReset() {
  const start = Date.now();
  const res = postRun(
    { query: '生成一份报告', model: 'mock-large' },
    { 'X-Mock-Fail-Rate': '1', 'X-Mock-Fail-Mode': 'half' },
    '40s');
  const cost = Date.now() - start;
  connResetDuration.add(cost);
  connResetHandled.add([200, 502, 504].includes(res.status) && cost < 30000);
  check(res, { '半断开连接受控收尾': (r) => [200, 502, 504].includes(r.status) });
}
