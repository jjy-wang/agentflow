import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const ttft = new Trend('ttft_ms');
const okRate = new Rate('run_ok');

export const options = {
  scenarios: {
    baseline: {
      executor: 'constant-vus',
      vus: 100,
      duration: '2m',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'ttft_ms': ['p(95)<1500'],
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  const start = Date.now();
  const res = http.post(`${BASE}/v1/agent/runs`,
    JSON.stringify({ query: '查询 10.0.0.1 这台主机的情况', model: 'mock-large' }),
    { headers: { 'Content-Type': 'application/json', 'X-Api-Key': 'demo-key-123456' } });
  const ok = check(res, { 'created': (r) => r.status === 200 });
  okRate.add(ok);
  if (ok) ttft.add(Date.now() - start);
  sleep(0.2);
}
