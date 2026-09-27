import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

/**
 * 동시 예약 방어(정합성 검증용) 스크립트.
 *
 * 같은 차종(vehicleType) + 지점(branchId)에 대해 동시에 VU_COUNT명이 예약을 시도한다.
 * reservation-service의 POST /api/reservations 자체는 재고를 확인하지 않고 항상 201을
 * 반환한다 - 이 프로젝트는 Choreography Saga라 실제 차량 배정은 Kafka를 거쳐 비동기로
 * (reservation-service -> payment-service -> vehicle-service) 일어난다. 그래서 이 스크립트는
 * 예약을 만든 뒤 각자 자기 예약 상태가 최종적으로 어떻게 결정되는지(PAYMENT_PENDING/CONFIRMED
 * = 차량을 실제로 받음, CANCELLED = 재고 부족으로 거절됨) 폴링해서 커스텀 지표로 센다.
 *
 * 이 스크립트를 실행하기 전에, 대상 (vehicleType, branchId) 조합의 실제 AVAILABLE 재고를
 * 정확히 1대로 맞춰둬야 한다(그래야 "N명 중 1명만 성공해야 정상"이라는 기대값이 명확해진다).
 * docs/concurrent-booking-lab.md 참고.
 *
 * 실행: BASE_URL=http://localhost:8081 BRANCH_ID=1111010100 VU_COUNT=20 \
 *       k6 run infra/k6/concurrent-booking-correctness.js
 */

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const BRANCH_ID = __ENV.BRANCH_ID || '1111010100';
const VU_COUNT = Number(__ENV.VU_COUNT || 20);

export const options = {
  scenarios: {
    concurrent_booking: {
      executor: 'shared-iterations',
      vus: VU_COUNT,
      iterations: VU_COUNT,
      maxDuration: '60s',
    },
  },
};

const gotVehicle = new Counter('reservation_got_vehicle');
const rejectedOutOfStock = new Counter('reservation_rejected_out_of_stock');
const timedOutWaiting = new Counter('reservation_status_timeout');

export default function () {
  const payload = JSON.stringify({
    customerId: `CONC-${__VU}-${__ITER}`,
    vehicleType: 'COMPACT',
    model: '모닝',
    branchId: BRANCH_ID,
    rentalStartAt: '2026-10-01T00:00:00Z',
    rentalEndAt: '2026-10-03T00:00:00Z',
    totalAmount: 100000, // fail-above-amount(100만원)보다 한참 낮게 - 가승인은 항상 성공,
                          // 차량 배정 결과만 보겠다는 뜻
  });

  const createRes = http.post(`${BASE_URL}/api/reservations`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(createRes, { '예약 생성 201': (r) => r.status === 201 });
  if (createRes.status !== 201) {
    return;
  }
  const reservationId = createRes.json('reservationId');

  // Saga가 끝날 때까지 폴링한다 (outbox 폴링 2초 주기 x 여러 hop을 거치므로 즉시 확정되지 않는다).
  for (let i = 0; i < 20; i++) {
    sleep(1);
    const getRes = http.get(`${BASE_URL}/api/reservations/${reservationId}`);
    const status = getRes.json('status');
    if (status === 'PAYMENT_PENDING' || status === 'CONFIRMED') {
      gotVehicle.add(1);
      return;
    }
    if (status === 'CANCELLED') {
      rejectedOutOfStock.add(1);
      return;
    }
  }
  timedOutWaiting.add(1);
}
