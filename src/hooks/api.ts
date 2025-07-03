/* ------------------------------------------------------------------ *
 *  API helper (REST + WebSocket) – includes JWT automatically
 * ------------------------------------------------------------------ */

import type {
  KPITile,
  Alert,
  AskAIResponse,
  MarketData,
  EfficiencyMetric,
  NewsItem,
} from "../types";

/* ------------------------------------------------------------------ *
 * Base URLs
 * ------------------------------------------------------------------ */

export const API_BASE =
  import.meta.env.VITE_API_URL || "http://localhost:8000";

const WS_BASE =
  import.meta.env.VITE_WS_BASE ||
  API_BASE.replace(/^http/, window.location.protocol === "https:" ? "wss" : "ws");

/* ------------------------------------------------------------------ *
 * Helpers
 * ------------------------------------------------------------------ */

// attach the bearer token (if logged-in)
const authHeaders = (): HeadersInit => {
  const t = localStorage.getItem("token");
  return t ? { Authorization: `Bearer ${t}` } : {};
};

// build absolute ws:// / wss:// URL
export const wsURL = (path: string) =>
  `${WS_BASE}${path.startsWith("/") ? path : `/${path}`}`;

/* ------------------------------------------------------------------ *
 *  Auth endpoints
 * ------------------------------------------------------------------ */

const AUTH_BASE = `${API_BASE}/auth`;

export const registerUser = async (body: {
  email: string;
  password: string;
  full_name?: string;
}): Promise<{ access_token: string; token_type: string }> => {
  const res = await fetch(`${AUTH_BASE}/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};

/** OAuth2 password-grant login – must be form-encoded */
export const loginUser = async (body: {
  email: string;
  password: string;
}): Promise<{ access_token: string; token_type: string }> => {
  const params = new URLSearchParams();
  params.append("username", body.email);   // OAuth2 uses “username”
  params.append("password", body.password);

  const res = await fetch(`${AUTH_BASE}/login`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: params.toString(),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};

/* ------------------------------------------------------------------ *
 *  Dashboard + alerts
 * ------------------------------------------------------------------ */

export const fetchDashboard = async (): Promise<KPITile[]> => {
  const res = await fetch(`${API_BASE}/dashboard`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};

export const fetchAlerts = async (): Promise<Alert[]> => {
  const res = await fetch(`${API_BASE}/alerts`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};

/* ------------------------------------------------------------------ *
 *  Market-data & efficiency
 * ------------------------------------------------------------------ */

export const fetchMarketData = async (): Promise<MarketData[]> => {
  const res = await fetch(`${API_BASE}/market-data`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};

export const fetchEfficiencyMetrics = async (): Promise<EfficiencyMetric[]> => {
  const res = await fetch(`${API_BASE}/efficiency`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};

/* ------------------------------------------------------------------ *
 *  AI & news
 * ------------------------------------------------------------------ */

export const askAI = async (body: {
  query: string;
}): Promise<AskAIResponse> => {
  const res = await fetch(`${API_BASE}/ask-ai`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};

export const fetchNews = async (): Promise<NewsItem[]> => {
  const res = await fetch(`${API_BASE}/news`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};
