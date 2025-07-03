/* ------------------------------------------------------------------ *
 * KPI / Metric
 * ------------------------------------------------------------------ */

export interface KPITile {
  label: string;
  value: number;
  delta_pct: number;
  spark?: number[];
  unit?: string;               // e.g. %, $,  
}

/** Rich metric object used in the dashboard UI */
export interface Metric {
  id: string;
  label: string;
  value: string | number;
  change: number;
  changeType: "percentage" | "absolute" | "multiplier";
  trend: "up" | "down" | "neutral";
  sparkline: number[];
  unit?: string;
  status?: "good" | "warning" | "critical";
  subtitle?: string;
}

/** Optional per-day efficiency record (used by future charts) */
export interface EfficiencyMetric {
  date: string;       // ISO date
  baseline: number;   // planned / target %
  actual: number;     // achieved %
  efficiency: number; // derived field (0-100)
}

/* ------------------------------------------------------------------ *
 * Alerts
 * ------------------------------------------------------------------ */

export type AlertSeverity = "critical" | "warning" | "info";
export type AlertCategory =
  | "sales"
  | "supply"
  | "competitor"
  | "market"
  | "finance"
  | "operations";

export type ActionType =
  | "marketing"
  | "logistics"
  | "analysis"
  | "finance"
  | "custom";
export type ActionStyle = "primary" | "danger" | "warning" | "default";

export interface AlertMetric {
  label: string;
  value: string;
  trend?: "up" | "down";
}

export interface AlertAction {
  id: string;
  label: string;
  icon?: string;
  type: ActionType;
  style: ActionStyle;
  automated?: boolean;
}

export interface Alert {
  id?: string;
  ts: string;                         // ISO timestamp
  severity: AlertSeverity;
  headline: string;
  title?: string;
  description?: string;
  details?: string;
  kpi?: string;
  suggested_action?: string;
  metrics?: AlertMetric[];
  actions?: AlertAction[];
  category?: AlertCategory;
}

/* ------------------------------------------------------------------ *
 * AI - Ask Response
 * ------------------------------------------------------------------ */

export interface AskAIResponse {
  impact_summary: string;
  query?: string;
  analysis?: {
    summary: string;
    confidence: number;
    sources: string[];
  };
  forecast: {
    dates: string[];
    baseline: number[];
    forecast: number[];
    lower: number[];
    upper: number[];
  };
  actions: {
    title: string;
    subtitle: string;
    cost: number;
    roi: number;
    cta: string;
  }[];
  predictions?: any;
  recommendations?: any[];
}

/* ------------------------------------------------------------------ *
 * Market / News
 * ------------------------------------------------------------------ */

export interface MarketData {
  id: string;
  entity: string;
  type: "price" | "trend" | "risk" | "launch" | "social";
  change: string;
  impact: string;
  timestamp: string;
  confidence?: number;
}

/** Simple news-feed item (future expansion) */
export interface NewsItem {
  id: string;
  headline: string;
  source: string;
  url?: string;
  timestamp: string;
  sentiment?: "positive" | "neutral" | "negative";
}

/* ------------------------------------------------------------------ *
 * Dashboard aggregate
 * ------------------------------------------------------------------ */

export interface DashboardData {
  metrics: Metric[];
  alerts: Alert[];
  marketData: MarketData[];

  forecast?: {
    dates: string[];
    baseline: number[];
    forecast: number[];
    lower: number[];
    upper: number[];
  };
  efficiency?: number;

  insights?: {
    icon: string;
    title: string;
    description: string;
    action: string;
  }[];

  lastUpdated: string; // ISO timestamp
}

/* ------------------------------------------------------------------ *
 * Users
 * ------------------------------------------------------------------ */

export interface User {
  id: string;
  name: string;
  email: string;
  role: string;
  company: string;
  initials: string;
  preferences?: {
    theme: "dark" | "light";
    density: "compact" | "comfortable" | "spacious";
    notifications: boolean;
  };
}

/* ------------------------------------------------------------------ *
 * WebSocket envelope
 * ------------------------------------------------------------------ */

export interface WSMessage {
  type: "alert" | "metric" | "market" | "system";
  data: any;
  timestamp: string;
}
