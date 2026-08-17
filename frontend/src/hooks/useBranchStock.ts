import { useEffect, useState } from "react";
import { getBranchStock } from "../api";
import type { VehicleType } from "../types";

const POLL_INTERVAL_MS = 2000;

export function useBranchStock(branchId: string | null) {
  const [stock, setStock] = useState<Record<VehicleType, number> | null>(null);

  useEffect(() => {
    if (!branchId) {
      setStock(null);
      return;
    }
    let cancelled = false;

    function refresh() {
      getBranchStock(branchId!)
        .then((data) => {
          if (!cancelled) {
            setStock(data);
          }
        })
        .catch(() => {
          if (!cancelled) {
            setStock(null);
          }
        });
    }

    refresh();
    // 예약이 생성돼도 실제 차량 배정(재고 감소)은 vehicle-service의 Outbox 폴링을 거쳐
    // 몇 초 뒤에 일어난다 — 예약 목록과 같은 주기로 폴링해야 그 변화를 바로 볼 수 있다.
    const timer = setInterval(refresh, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [branchId]);

  return stock;
}
