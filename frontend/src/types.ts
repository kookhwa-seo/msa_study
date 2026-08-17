export type VehicleType = "COMPACT" | "SUV" | "VAN";

export const VEHICLE_TYPE_LABELS: Record<VehicleType, string> = {
  COMPACT: "소형",
  SUV: "SUV",
  VAN: "승합차",
};

export type ReservationStatus = "PENDING" | "PAYMENT_PENDING" | "CONFIRMED" | "CANCELLED";

export interface Branch {
  code: string;
  sido: string;
  sigungu: string;
  dong: string;
}

export interface Reservation {
  reservationId: string;
  customerId: string;
  vehicleType: VehicleType;
  model: string;
  branchId: string;
  rentalStartAt: string;
  rentalEndAt: string;
  totalAmount: number;
  status: ReservationStatus;
  createdAt: string;
  updatedAt: string;
}

export interface CreateReservationRequest {
  customerId: string;
  vehicleType: VehicleType;
  model: string;
  branchId: string;
  rentalStartAt: string;
  rentalEndAt: string;
  totalAmount: number;
}

export interface ChatState {
  customerId: string | null;
  vehicleType: VehicleType | null;
  model: string | null;
  branchId: string | null;
  rentalStartAt: string | null;
  rentalEndAt: string | null;
  totalAmount: number | null;
}

export interface ChatResponse {
  reply: string;
  state: ChatState;
  missingFields: string[];
  readyToSubmit: boolean;
}

export const EMPTY_CHAT_STATE: ChatState = {
  customerId: null,
  vehicleType: null,
  model: null,
  branchId: null,
  rentalStartAt: null,
  rentalEndAt: null,
  totalAmount: null,
};
