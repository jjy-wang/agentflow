import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    sse: {
      executor: 'constant-vus',
      vus: 10000,          // 1 万并发连接
      duration: '5m',
      gracefulStop: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  // 慢读：timeout 拉长，模拟真实长连接客户端
  const res = http.get(`${BASE}/v1/agent/runs/fake/events`, {
    headers: { 'X-Api-Key': 'demo-key-123456', 'Last-Event-ID': '0' },
    timeout: '300s',
  });
  check(res, { 'connected': (r) => r.status === 200 || r.status === 401 });
}
