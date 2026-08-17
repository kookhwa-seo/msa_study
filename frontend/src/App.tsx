import { NavLink, Route, Routes } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import Chat from "./pages/Chat";

export default function App() {
  return (
    <div className="app-shell">
      <header className="top-nav">
        <span className="brand">MSA Rental Study</span>
        <nav>
          <NavLink to="/" end>
            대시보드
          </NavLink>
          <NavLink to="/chat">예약 어시스턴트</NavLink>
        </nav>
      </header>
      <main>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/chat" element={<Chat />} />
        </Routes>
      </main>
    </div>
  );
}
