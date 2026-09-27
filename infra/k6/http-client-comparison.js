import http from 'k6/http';
import { check } from 'k6';

/**
 * 블로킹(RestClient) vs 논블로킹(WebClient) vs 가상 스레드 비교 실습.
 * VU_COUNT 하나를 일정 시간 동안 유지하면서 TPS/p99/에러율을 잰다 - 동시 사용자 수별로
 * 깨끗하게 분리된 결과를 얻으려고, 50/100/200/500을 한 번에 램프업하는 대신 이 스크립트를
 * VU_COUNT만 바꿔가며 4번 따로 돌린다.
 *
 * 실행: BASE_URL=http://localhost:8088 TARGET_PATH=/blocking VU_COUNT=100 \
 *       k6 run infra/k6/http-client-comparison.js
 */

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8088';
const TARGET_PATH = __ENV.TARGET_PATH || '/blocking';
const VU_COUNT = Number(__ENV.VU_COUNT || 50);

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    load: {
      executor: 'constant-vus',
      vus: VU_COUNT,
      duration: '20s',
    },
  },
};

export default function () {
  const res = http.post(`${BASE_URL}${TARGET_PATH}`, null);
  check(res, { '200 OK': (r) => r.status === 200 });
}
