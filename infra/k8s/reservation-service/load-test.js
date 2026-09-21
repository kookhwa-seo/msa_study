import http from 'k6/http';
import { sleep } from 'k6';

// 오토스케일링 실습용 부하 테스트. reservation-service의 실제 비즈니스 API
// (GET /api/reservations)를 대상으로, 동시 사용자 수를 늘렸다가 줄이면서
// HPA가 파드를 스케일 아웃/인 하는 걸 관찰한다.
export const options = {
  stages: [
    { duration: '30s', target: 40 },   // 0 -> 40 VU로 램프업
    { duration: '2m', target: 40 },    // 40 VU 유지 (HPA가 반응할 시간을 준다)
    { duration: '30s', target: 0 },    // 다시 0으로 램프다운
  ],
};

const BASE_URL = 'http://localhost:18081';

export default function () {
  http.get(`${BASE_URL}/api/reservations`);
  sleep(0.1);
}
