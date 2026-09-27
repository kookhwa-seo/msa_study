import http from 'k6/http';
import { check } from 'k6';

/**
 * reservation-service의 POST /api/reservations 자체(동기 write 경로)의 처리량을 재는
 * 램프업 부하 테스트. 10 -> 50 -> 100 -> 200명까지 단계적으로 올린다.
 *
 * 주의: 동시 예약 방어에 쓴 락(비관적 락)은 이 API가 아니라 vehicle-service의 비동기 Kafka
 * 컨슈머(handlePaymentAuthorized) 안에 있다. 그래서 이 스크립트의 TPS/레이턴시는 락 적용
 * 전후로 사실상 차이가 없다 - 그 자체가 이 실습에서 확인하려는 것 중 하나다
 * (docs/concurrent-booking-lab.md 참고). 락이 실제로 거는 성능 비용은
 * VehicleAssignmentConcurrencyTest가 재는 배정 처리 시간 쪽에서 확인한다.
 *
 * 실행: BASE_URL=http://localhost:8081 k6 run infra/k6/reservation-ramp-up.js
 */

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  stages: [
    { duration: '30s', target: 10 },
    { duration: '30s', target: 50 },
    { duration: '30s', target: 100 },
    { duration: '30s', target: 200 },
    { duration: '15s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.05'],
  },
};

let seq = 0;

export default function () {
  seq += 1;
  const payload = JSON.stringify({
    customerId: `RAMP-${__VU}-${seq}`,
    vehicleType: 'COMPACT',
    model: '모닝',
    branchId: '1111010100',
    rentalStartAt: '2026-11-01T00:00:00Z',
    rentalEndAt: '2026-11-03T00:00:00Z',
    totalAmount: 100000,
  });

  const res = http.post(`${BASE_URL}/api/reservations`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { '201 Created': (r) => r.status === 201 });
}
