import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  scenarios: {
    soak: { executor: 'constant-vus', vus: 30, duration: '30m' },
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  http.post(`${BASE}/v1/agent/runs`,
    JSON.stringify({ query: '查询漏洞', model: 'mock-large' }),
    { headers: { 'Content-Type': 'application/json', 'X-Api-Key': 'demo-key-123456' } });
  sleep(1);
}
