import type { ReservationStatus } from "../types";

const LABELS: Record<ReservationStatus, string> = {
  PENDING: "대기중",
  PAYMENT_PENDING: "결제대기",
  CONFIRMED: "확정",
  CANCELLED: "취소",
};

const CLASS_NAMES: Record<ReservationStatus, string> = {
  PENDING: "badge badge-pending",
  PAYMENT_PENDING: "badge badge-payment-pending",
  CONFIRMED: "badge badge-confirmed",
  CANCELLED: "badge badge-cancelled",
};

export default function StatusBadge({ status }: { status: ReservationStatus }) {
  return <span className={CLASS_NAMES[status]}>{LABELS[status]}</span>;
}
