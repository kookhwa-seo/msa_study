import { useEffect, useMemo, useState, type FormEvent } from "react";
import { createReservation, listReservations } from "../api";
import BranchPicker from "../components/BranchPicker";
import StatusBadge from "../components/StatusBadge";
import StockSummary from "../components/StockSummary";
import { formatBranch, useBranches } from "../hooks/useBranches";
import { useBranchStock } from "../hooks/useBranchStock";
import { VEHICLE_TYPE_LABELS, type CreateReservationRequest, type Reservation, type VehicleType } from "../types";

const VEHICLE_TYPES: VehicleType[] = ["COMPACT", "SUV", "VAN"];

const POLL_INTERVAL_MS = 2000;

function toInstantAtSeoulMidnight(dateStr: string): string {
  return new Date(`${dateStr}T00:00:00+09:00`).toISOString();
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString("ko-KR", { year: "numeric", month: "2-digit", day: "2-digit" });
}

function todayPlusDays(days: number): string {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return date.toISOString().slice(0, 10);
}

const initialForm = {
  customerId: "CUST-WEB-1",
  vehicleType: "SUV" as VehicleType,
  model: "",
  branchId: "",
  rentalStartAt: todayPlusDays(7),
  rentalEndAt: todayPlusDays(9),
  totalAmount: "200000",
};

export default function Dashboard() {
  const [reservations, setReservations] = useState<Reservation[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [form, setForm] = useState(initialForm);
  const [submitting, setSubmitting] = useState(false);
  const { branches, byCode } = useBranches();
  const stock = useBranchStock(form.branchId || null);

  useEffect(() => {
    let cancelled = false;

    async function refresh() {
      try {
        const data = await listReservations();
        if (!cancelled) {
          setReservations(data);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "예약 목록을 불러오지 못했습니다.");
        }
      }
    }

    refresh();
    const timer = setInterval(refresh, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  const sortedReservations = useMemo(
    () => [...reservations].sort((a, b) => b.createdAt.localeCompare(a.createdAt)),
    [reservations]
  );

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    if (!form.branchId) {
      setError("지점을 선택해주세요.");
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      const request: CreateReservationRequest = {
        customerId: form.customerId,
        vehicleType: form.vehicleType,
        model: form.model,
        branchId: form.branchId,
        rentalStartAt: toInstantAtSeoulMidnight(form.rentalStartAt),
        rentalEndAt: toInstantAtSeoulMidnight(form.rentalEndAt),
        totalAmount: Number(form.totalAmount),
      };
      const created = await createReservation(request);
      setReservations((prev) => [created, ...prev]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "예약 생성에 실패했습니다.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="page">
      <section className="card">
        <h2>새 예약 만들기</h2>
        <p className="muted">
          Saga는 비동기로 진행됩니다. 제출 후 아래 목록이 2초마다 자동 새로고침되며
          PENDING → PAYMENT_PENDING → CONFIRMED/CANCELLED로 상태가 바뀌는 것을 지켜볼 수 있습니다.
        </p>
        <form className="form-grid" onSubmit={handleSubmit}>
          <label>
            고객 ID
            <input
              value={form.customerId}
              onChange={(e) => setForm({ ...form, customerId: e.target.value })}
              required
            />
          </label>
          <label>
            차량 종류
            <select
              value={form.vehicleType}
              onChange={(e) => setForm({ ...form, vehicleType: e.target.value as VehicleType })}
            >
              {VEHICLE_TYPES.map((type) => (
                <option key={type} value={type}>
                  {VEHICLE_TYPE_LABELS[type]}
                </option>
              ))}
            </select>
          </label>
          <label>
            모델명
            <input
              value={form.model}
              onChange={(e) => setForm({ ...form, model: e.target.value })}
              placeholder="예: 쏘나타, 카니발"
              required
            />
          </label>
          <label className="field-wide">
            지점
            <BranchPicker branches={branches} onChange={(code) => setForm((f) => ({ ...f, branchId: code }))} />
            <StockSummary stock={stock} />
          </label>
          <label>
            대여 시작일
            <input
              type="date"
              value={form.rentalStartAt}
              onChange={(e) => setForm({ ...form, rentalStartAt: e.target.value })}
              required
            />
          </label>
          <label>
            대여 종료일
            <input
              type="date"
              value={form.rentalEndAt}
              onChange={(e) => setForm({ ...form, rentalEndAt: e.target.value })}
              required
            />
          </label>
          <label>
            결제 예정 금액(원)
            <input
              type="number"
              min={1}
              value={form.totalAmount}
              onChange={(e) => setForm({ ...form, totalAmount: e.target.value })}
              required
            />
          </label>
          <button type="submit" disabled={submitting}>
            {submitting ? "예약 생성 중..." : "예약 생성"}
          </button>
        </form>
        {stock && stock[form.vehicleType] === 0 && (
          <p className="stock-warning">
            선택한 지점에는 {VEHICLE_TYPE_LABELS[form.vehicleType]} 재고가 없습니다 — 위 재고 현황에서
            재고가 있는 차종이나 다른 지점을 선택하면 배정 실패 없이 예약할 수 있습니다.
          </p>
        )}
      </section>

      {error && <div className="error-banner">{error}</div>}

      <section className="card">
        <h2>예약 목록 ({sortedReservations.length})</h2>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>예약 ID</th>
                <th>고객</th>
                <th>차종</th>
                <th>모델</th>
                <th>지점</th>
                <th>대여 기간</th>
                <th>금액</th>
                <th>상태</th>
              </tr>
            </thead>
            <tbody>
              {sortedReservations.map((r) => (
                <tr key={r.reservationId}>
                  <td className="mono" title={r.reservationId}>
                    {r.reservationId.slice(0, 8)}…
                  </td>
                  <td>{r.customerId}</td>
                  <td>{VEHICLE_TYPE_LABELS[r.vehicleType]}</td>
                  <td>{r.model}</td>
                  <td>{formatBranch(byCode.get(r.branchId))}</td>
                  <td>
                    {formatDate(r.rentalStartAt)} ~ {formatDate(r.rentalEndAt)}
                  </td>
                  <td>{r.totalAmount.toLocaleString("ko-KR")}원</td>
                  <td>
                    <StatusBadge status={r.status} />
                  </td>
                </tr>
              ))}
              {sortedReservations.length === 0 && (
                <tr>
                  <td colSpan={8} className="muted center">
                    아직 예약이 없습니다.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
