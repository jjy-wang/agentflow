import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const rejected = new Counter('quota_rejected');

export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 400,            // 每秒 400 次，远超 rpm=600
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 200,
      maxVUs: 600,
    },
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  const res = http.post(`${BASE}/v1/agent/runs`,
    JSON.stringify({ query: '查询资产', model: 'mock-large' }),
    { headers: { 'Content-Type': 'application/json', 'X-Api-Key': 'demo-key-123456' } });
  if (res.status === 429) {
    rejected.add(1);
  }
  check(res, { 'status is 200 or 429': (r) => r.status === 200 || r.status === 429 });
}
