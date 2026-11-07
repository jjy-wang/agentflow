import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

const endToEnd = new Trend('run_e2e_ms');

export const options = {
  scenarios: {
    multi: { executor: 'constant-vus', vus: 50, duration: '3m' },
  },
  thresholds: { 'run_e2e_ms': ['p(95)<8000'] },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const HEADERS = {
  'Content-Type': 'application/json',
  'X-Api-Key': 'demo-key-123456',
  // 让 mock 返回一次工具调用，走完整 ReAct 链路
  'X-Mock-Tool': 'asset_search',
  'X-Mock-Tokens': '64',
  'X-Mock-Tps': '40',
};

export default function () {
  const start = Date.now();
  const res = http.post(`${BASE}/v1/agent/runs`, JSON.stringify({
    query: '帮我看看有哪些主机，然后汇总一下',
    model: 'mock-large',
    sessionId: 1,
  }), { headers: HEADERS });
  check(res, { 'ok': (r) => r.status === 200 });
  endToEnd.add(Date.now() - start);
}
