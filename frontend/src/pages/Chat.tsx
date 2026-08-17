import { useEffect, useRef, useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";
import { createReservation, sendChatMessage } from "../api";
import StockSummary from "../components/StockSummary";
import { formatBranch, useBranches } from "../hooks/useBranches";
import { useBranchStock } from "../hooks/useBranchStock";
import { EMPTY_CHAT_STATE, VEHICLE_TYPE_LABELS, type Branch, type ChatState } from "../types";

interface Message {
  role: "user" | "assistant";
  text: string;
}

const GREETING: Message = {
  role: "assistant",
  text: '안녕하세요! 예약하고 싶은 내용을 자유롭게 말씀해주세요. 전국 어디든 지역을 알려주시면 됩니다.\n예: "다음주 화요일부터 2박, 왕십리에서 SUV, 20만원 정도로 렌트하고 싶어"',
};

const FIELD_LABELS: Record<keyof ChatState, string> = {
  customerId: "고객 ID",
  vehicleType: "차량 종류",
  model: "모델명",
  branchId: "지점",
  rentalStartAt: "대여 시작일",
  rentalEndAt: "대여 종료일",
  totalAmount: "결제 예정 금액",
};

const SLOT_KEYS = (Object.keys(FIELD_LABELS) as (keyof ChatState)[]).filter((key) => key !== "customerId");

function formatSlotValue(
  key: keyof ChatState,
  value: ChatState[keyof ChatState],
  branch: Branch | undefined
): string {
  if (value == null) {
    return "미확인";
  }
  if (key === "vehicleType") {
    return VEHICLE_TYPE_LABELS[value as keyof typeof VEHICLE_TYPE_LABELS];
  }
  if (key === "branchId") {
    return formatBranch(branch);
  }
  if (key === "rentalStartAt" || key === "rentalEndAt") {
    return new Date(value as string).toLocaleDateString("ko-KR");
  }
  if (key === "totalAmount") {
    return `${Number(value).toLocaleString("ko-KR")}원`;
  }
  return String(value);
}

export default function Chat() {
  const navigate = useNavigate();
  const [customerId, setCustomerId] = useState("CUST-WEB-1");
  const [chatState, setChatState] = useState<ChatState>({ ...EMPTY_CHAT_STATE, customerId: "CUST-WEB-1" });
  const [messages, setMessages] = useState<Message[]>([GREETING]);
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const [readyToSubmit, setReadyToSubmit] = useState(false);
  const [missingFields, setMissingFields] = useState<string[]>([]);
  const [confirming, setConfirming] = useState(false);
  const [confirmedId, setConfirmedId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const { byCode } = useBranches();
  const stock = useBranchStock(chatState.branchId);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [messages]);

  function handleReset() {
    setMessages([GREETING]);
    setChatState({ ...EMPTY_CHAT_STATE, customerId });
    setMissingFields([]);
    setReadyToSubmit(false);
    setConfirmedId(null);
    setError(null);
    setInput("");
  }

  async function handleSend(e: FormEvent) {
    e.preventDefault();
    if (!input.trim() || sending) {
      return;
    }
    const userMessage = input.trim();
    setMessages((prev) => [...prev, { role: "user", text: userMessage }]);
    setInput("");
    setSending(true);
    setError(null);
    try {
      const stateToSend = { ...chatState, customerId };
      const response = await sendChatMessage(userMessage, stateToSend);
      setChatState(response.state);
      setMissingFields(response.missingFields);
      setReadyToSubmit(response.readyToSubmit);
      setMessages((prev) => [...prev, { role: "assistant", text: response.reply }]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "채팅 요청에 실패했습니다.");
    } finally {
      setSending(false);
    }
  }

  async function handleConfirm() {
    if (
      !chatState.customerId ||
      !chatState.vehicleType ||
      !chatState.model ||
      !chatState.branchId ||
      !chatState.rentalStartAt ||
      !chatState.rentalEndAt ||
      chatState.totalAmount == null
    ) {
      return;
    }
    setConfirming(true);
    setError(null);
    try {
      const reservation = await createReservation({
        customerId: chatState.customerId,
        vehicleType: chatState.vehicleType,
        model: chatState.model,
        branchId: chatState.branchId,
        rentalStartAt: chatState.rentalStartAt,
        rentalEndAt: chatState.rentalEndAt,
        totalAmount: chatState.totalAmount,
      });
      setConfirmedId(reservation.reservationId);
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          text: `예약이 접수됐어요! 예약 ID: ${reservation.reservationId}\n대시보드에서 상태 변화를 확인해보세요.`,
        },
      ]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "예약 생성에 실패했습니다.");
    } finally {
      setConfirming(false);
    }
  }

  return (
    <div className="page chat-layout">
      <section className="card chat-panel">
        <div className="chat-panel-header">
          <div>
            <h2>예약 어시스턴트</h2>
            <p className="muted">로컬 LLM(Ollama)이 문장에서 예약 정보를 추출합니다.</p>
          </div>
          <button type="button" className="ghost-button" onClick={handleReset}>
            새 대화 시작
          </button>
        </div>
        <div className="chat-messages">
          {messages.map((m, i) => (
            <div key={i} className={`chat-bubble ${m.role}`}>
              {m.text.split("\n").map((line, j) => (
                <div key={j}>{line}</div>
              ))}
            </div>
          ))}
          <div ref={messagesEndRef} />
        </div>
        <form className="chat-input-row" onSubmit={handleSend}>
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="예약하고 싶은 내용을 입력하세요"
            disabled={sending}
          />
          <button type="submit" disabled={sending || !input.trim()}>
            {sending ? "전송 중..." : "전송"}
          </button>
        </form>
      </section>

      <aside className="card chat-side">
        <h3>고객 ID</h3>
        <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} />

        <h3>추출된 예약 정보</h3>
        <ul className="slot-list">
          {SLOT_KEYS.map((key) => (
            <li key={key} className={missingFields.includes(key) ? "slot missing" : "slot filled"}>
              <span>{FIELD_LABELS[key]}</span>
              <span>{formatSlotValue(key, chatState[key], byCode.get(chatState.branchId ?? ""))}</span>
            </li>
          ))}
        </ul>
        <StockSummary stock={stock} />

        {error && <div className="error-banner">{error}</div>}
        {stock && chatState.vehicleType && stock[chatState.vehicleType] === 0 && (
          <p className="stock-warning">
            이 지점에는 {VEHICLE_TYPE_LABELS[chatState.vehicleType]} 재고가 없어요 — 다른 차종이나
            지점을 말씀해주시면 배정 실패 없이 예약할 수 있어요.
          </p>
        )}

        {confirmedId ? (
          <div className="success-banner">
            예약 확정 요청 완료 (ID: {confirmedId.slice(0, 8)}…)
            <button type="button" onClick={() => navigate("/")}>
              대시보드에서 보기
            </button>
          </div>
        ) : (
          <button type="button" disabled={!readyToSubmit || confirming} onClick={handleConfirm}>
            {confirming ? "예약 확정 중..." : "예약 확정하기"}
          </button>
        )}
      </aside>
    </div>
  );
}
