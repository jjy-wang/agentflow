import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    slow_reader: {
      executor: 'constant-vus',
      vus: 200,
      duration: '2m',
    },
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  const res = http.post(`${BASE}/v1/agent/runs`, JSON.stringify({
    query: '生成一份很长的报告',
    model: 'mock-large',
  }), {
    headers: {
      'Content-Type': 'application/json',
      'X-Api-Key': 'demo-key-123456',
      'X-Mock-Tps': '500',       // 上游吐得极快
      'X-Mock-Tokens': '2000',
    },
    timeout: '120s',
  });
  check(res, { 'no oom': (r) => r.status === 200 });
}
