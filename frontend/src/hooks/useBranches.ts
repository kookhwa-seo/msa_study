import { useEffect, useMemo, useState } from "react";
import { listBranches } from "../api";
import type { Branch } from "../types";

export function formatBranch(branch: Branch | undefined): string {
  if (!branch) {
    return "알 수 없는 지점";
  }
  return `${branch.sido} ${branch.sigungu} ${branch.dong}`;
}

export function useBranches() {
  const [branches, setBranches] = useState<Branch[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    listBranches()
      .then((data) => {
        if (!cancelled) {
          setBranches(data);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "지점 목록을 불러오지 못했습니다.");
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const byCode = useMemo(() => {
    const map = new Map<string, Branch>();
    branches.forEach((b) => map.set(b.code, b));
    return map;
  }, [branches]);

  return { branches, byCode, error };
}
