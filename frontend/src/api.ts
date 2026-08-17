import type { Branch, ChatResponse, ChatState, CreateReservationRequest, Reservation, VehicleType } from "./types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8085";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`${response.status} ${response.statusText}${body ? `: ${body}` : ""}`);
  }
  return response.json() as Promise<T>;
}

export function listReservations(): Promise<Reservation[]> {
  return request<Reservation[]>("/api/reservations");
}

export function listBranches(): Promise<Branch[]> {
  return request<Branch[]>("/api/branches");
}

export function getBranchStock(branchId: string): Promise<Record<VehicleType, number>> {
  return request<Record<VehicleType, number>>(`/api/branches/${branchId}/stock`);
}

export function createReservation(body: CreateReservationRequest): Promise<Reservation> {
  return request<Reservation>("/api/reservations", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function sendChatMessage(message: string, state: ChatState): Promise<ChatResponse> {
  return request<ChatResponse>("/api/chat", {
    method: "POST",
    body: JSON.stringify({ message, state }),
  });
}
