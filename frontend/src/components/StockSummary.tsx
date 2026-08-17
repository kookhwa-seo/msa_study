import { VEHICLE_TYPE_LABELS, type VehicleType } from "../types";

const TYPES: VehicleType[] = ["COMPACT", "SUV", "VAN"];

export default function StockSummary({ stock }: { stock: Record<VehicleType, number> | null }) {
  if (!stock) {
    return null;
  }
  return (
    <ul className="stock-summary">
      {TYPES.map((type) => (
        <li key={type} className={stock[type] > 0 ? "stock-ok" : "stock-empty"}>
          {VEHICLE_TYPE_LABELS[type]} {stock[type]}대
        </li>
      ))}
    </ul>
  );
}
