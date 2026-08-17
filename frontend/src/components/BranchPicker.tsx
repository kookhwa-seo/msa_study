import { useEffect, useMemo, useState } from "react";
import type { Branch } from "../types";

interface BranchPickerProps {
  branches: Branch[];
  onChange: (code: string) => void;
}

export default function BranchPicker({ branches, onChange }: BranchPickerProps) {
  const sidoList = useMemo(() => Array.from(new Set(branches.map((b) => b.sido))), [branches]);
  const [sido, setSido] = useState("");

  useEffect(() => {
    if (!sido && sidoList.length > 0) {
      setSido(sidoList[0]);
    }
  }, [sido, sidoList]);

  const sigunguList = useMemo(
    () => Array.from(new Set(branches.filter((b) => b.sido === sido).map((b) => b.sigungu))),
    [branches, sido]
  );
  const [sigungu, setSigungu] = useState("");

  useEffect(() => {
    if (sigunguList.length > 0 && !sigunguList.includes(sigungu)) {
      setSigungu(sigunguList[0]);
    }
  }, [sigunguList, sigungu]);

  const dongList = useMemo(
    () => branches.filter((b) => b.sido === sido && b.sigungu === sigungu),
    [branches, sido, sigungu]
  );
  const [dongCode, setDongCode] = useState("");

  useEffect(() => {
    if (dongList.length > 0 && !dongList.some((d) => d.code === dongCode)) {
      setDongCode(dongList[0].code);
    }
  }, [dongList, dongCode]);

  useEffect(() => {
    if (dongCode) {
      onChange(dongCode);
    }
    // dongCode가 바뀔 때만 알리면 된다. onChange까지 의존성에 넣으면 부모가 매 렌더마다
    // 새 함수를 넘길 때(예: 인라인 화살표 함수) 여기서 다시 호출 -> 부모 재렌더 -> 새 함수
    // 생성이 반복되는 무한 루프가 생긴다.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dongCode]);

  if (branches.length === 0) {
    return <p className="muted">지점 목록을 불러오는 중...</p>;
  }

  return (
    <div className="branch-picker">
      <select
        value={sido}
        onChange={(e) => {
          setSido(e.target.value);
          setSigungu("");
          setDongCode("");
        }}
      >
        {sidoList.map((s) => (
          <option key={s} value={s}>
            {s}
          </option>
        ))}
      </select>
      <select
        value={sigungu}
        onChange={(e) => {
          setSigungu(e.target.value);
          setDongCode("");
        }}
      >
        {sigunguList.map((s) => (
          <option key={s} value={s}>
            {s}
          </option>
        ))}
      </select>
      <select value={dongCode} onChange={(e) => setDongCode(e.target.value)}>
        {dongList.map((d) => (
          <option key={d.code} value={d.code}>
            {d.dong}
          </option>
        ))}
      </select>
    </div>
  );
}
