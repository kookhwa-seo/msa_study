import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

/**
 * Rate limiting 실습(docs/rate-limiting-lab.md) 전용 스파이크 테스트. 500명이 사실상 동시에
 * (램프업 없이) POST /api/reservations를 한 번씩 쏜다 - "인기 지점이 갑자기 풀려서 다들
 * 동시에 예약 버튼을 누르는" 상황을 흉내낸다.
 *
 * 실행: BASE_URL=http://localhost:8081 k6 run infra/k6/reservation-spike.js
 */

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    spike: {
      executor: 'per-vu-iterations',
      vus: 500,
      iterations: 1,
      maxDuration: '30s',
    },
  },
};

const created = new Counter('reservation_created_201');
const rateLimited = new Counter('reservation_rate_limited_429');
const otherStatus = new Counter('reservation_other_status');

export default function () {
  const payload = JSON.stringify({
    customerId: `SPIKE-${__VU}`,
    vehicleType: 'COMPACT',
    model: '모닝',
    branchId: '9999999999',
    rentalStartAt: '2026-12-01T00:00:00Z',
    rentalEndAt: '2026-12-02T00:00:00Z',
    totalAmount: 100000,
  });

  const res = http.post(`${BASE_URL}/api/reservations`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  if (res.status === 201) {
    created.add(1);
  } else if (res.status === 429) {
    rateLimited.add(1);
  } else {
    otherStatus.add(1);
  }
  check(res, { '201 또는 429': (r) => r.status === 201 || r.status === 429 });
}
