import http from 'k6/http';
import { check } from 'k6';

/**
 * 가용 차량 수 캐싱 실습(docs/availability-cache-lab.md) 전/후 비교. 같은 지점의 재고를
 * 반복 조회하는 상황(인기 지점 상세 페이지에 여러 사용자가 몰리는 것과 비슷하다)을 흉내낸다.
 *
 * 실행:
 *   BASE_URL=http://localhost:8082 BRANCH_ID=3014010100 k6 run infra/k6/availability-cache-comparison.js
 */

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8082';
const BRANCH_ID = __ENV.BRANCH_ID || '3014010100';

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    stock_lookup: {
      executor: 'constant-vus',
      vus: 100,
      duration: '20s',
    },
  },
};

export default function () {
  const res = http.get(`${BASE_URL}/api/branches/${BRANCH_ID}/stock`);
  check(res, { '200 OK': (r) => r.status === 200 });
}
