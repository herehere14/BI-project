import React, { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { useNavigate } from "react-router-dom";

/* ────────────────────────────────────────────────────────────
   Components
   ───────────────────────────────────────────────────────── */
import Layouts from "../components/Layouts";
import Sidebar from "../components/dashboard/Sidebar";
import MetricsBar from "../components/dashboard/MetricsBar";
import AlertStream from "../components/dashboard/AlertStream";
import ForecastChart from "../components/dashboard/ForecastChart";
import EfficiencyGauge from "../components/dashboard/EfficiencyGauge";
import MarketIntel from "../components/dashboard/MarketIntel";
import NewsTicker from "../components/dashboard/NewsTicker";
import SearchBar from "../components/SearchBar";

/* ────────────────────────────────────────────────────────────
   Hooks & Types
   ───────────────────────────────────────────────────────── */
import { useDashboardData } from "../hooks/useDashboardData";
import { useMarketData } from "../hooks/useMarketData";
import { Alert } from "../types";

/* ────────────────────────────────────────────────────────────
   Dashboard Page
   ───────────────────────────────────────────────────────── */
const Dashboard: React.FC = () => {
  const navigate = useNavigate();

  /* Fetch core data */
  const { data: dashboardData, isLoading, error } = useDashboardData();
  const { data: marketData } = useMarketData();

  /* UI state */
  const [selectedAlert, setSelectedAlert] = useState<Alert | null>(null);
  const [activeView, setActiveView] = useState<
    "overview" | "analysis" | "automation"
  >("overview");
  const [isFullscreen, setIsFullscreen] = useState(false);

  /* Optional live-update socket (stubbed) */
  useEffect(() => {
    const ws = new WebSocket(
      import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws/dashboard",
    );
    ws.onmessage = (ev) => console.log("Dashboard update:", ev.data);
    return () => ws.close();
  }, []);

  /* Handle quick-action buttons from AlertStream */
  const handleAlertAction = (action: string, alertId: string) => {
    const alert = dashboardData?.alerts.find((a) => a.id === alertId);
    if (!alert) return;

    switch (action) {
      case "marketing":
        console.info("Open marketing automation:", alert);
        break;
      case "logistics":
        console.info("Open logistics automation:", alert);
        break;
      case "analysis":
        setSelectedAlert(alert);
        setActiveView("analysis");
        break;
      case "forecast":
        navigate("/forecasts", { state: { alert } });
        break;
      default:
        console.warn("Unknown action:", action);
    }
  };

  /* ───────── Loading & Error guards ──────── */
  if (isLoading) {
    return (
      <Layouts>
        <Sidebar />
        <div className="flex flex-1 items-center justify-center">
          <motion.div
            animate={{ rotate: 360 }}
            transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
            className="w-12 h-12 border-3 border-primary border-t-transparent rounded-full mr-4"
          />
          <p className="text-muted-foreground">Loading Decision Intelligence…</p>
        </div>
        <div /> {/* placeholder right rail */}
      </Layouts>
    );
  }

  if (error) {
    return (
      <Layouts>
        <Sidebar />
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <h2 className="text-xl font-semibold text-destructive mb-2">
              Connection Error
            </h2>
            <p className="text-muted-foreground mb-4">
              Unable to connect to data stream
            </p>
            <button onClick={() => window.location.reload()} className="btn-primary">
              Retry Connection
            </button>
          </div>
        </div>
        <div />
      </Layouts>
    );
  }

  /* ───────────────────────────────────────────
     Happy-path render
     ───────────────────────────────────────── */
  const metrics   = dashboardData?.metrics ?? [];
  const alerts    = dashboardData?.alerts  ?? [];
  const forecast  = dashboardData?.forecast;
  const efficiency= dashboardData?.efficiency ?? 0;
  const insights  = dashboardData?.insights  ?? [];

  return (
    <Layouts>
      {/* ───────── Left rail ───────── */}
      <Sidebar />

      {/* ───────── Centre column ───────── */}
      <section className="flex flex-col min-w-0 overflow-hidden">
        {/* Top bar: search & view toggles */}
        <header className="border-b border-border bg-card">
          <div className="flex items-center justify-between gap-4 px-6 py-4">
            <SearchBar />

            {/* View tabs */}
            <div className="flex gap-2">
              {["overview", "analysis", "automation"].map((v) => (
                <button
                  key={v}
                  onClick={() => setActiveView(v as any)}
                  className={`px-3 py-1.5 text-xs font-medium rounded transition-all ${
                    activeView === v
                      ? "bg-primary text-primary-foreground"
                      : "text-muted-foreground hover:text-foreground"
                  }`}
                >
                  {v.toUpperCase()}
                </button>
              ))}
            </div>

            {/* Quick-links */}
            <div className="flex gap-2">
              <button
                onClick={() => navigate("/opportunities")}
                className="btn-ghost text-xs"
              >
                VIEW OPPORTUNITIES
              </button>
              <button
                onClick={() => setIsFullscreen(!isFullscreen)}
                className="btn-ghost text-xs"
              >
                {isFullscreen ? "EXIT FULLSCREEN" : "FULLSCREEN"}
              </button>
            </div>
          </div>
        </header>

        {/* Metrics strip */}
        <div className="border-b border-border">
          <MetricsBar metrics={metrics} />
        </div>

        {/* Main switch-able panels */}
        <div className="flex-1 overflow-auto">
          {activeView === "overview" && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="p-6 space-y-6"
            >
              {/* Alerts + Charts grid */}
              <div className="grid lg:grid-cols-12 gap-6">
                {/* Alerts (4) */}
                <div className="lg:col-span-4">
                  <div className="bg-card h-[600px] border border-border rounded-lg">
                    <AlertStream alerts={alerts} onAction={handleAlertAction} />
                  </div>
                </div>

                {/* Charts (8) */}
                <div className="lg:col-span-8 space-y-6">
                  <div className="bg-card border border-border rounded-lg p-4 h-[290px]">
                    <h3 className="text-xs font-bold uppercase text-muted-foreground mb-3">
                      Revenue Forecast &amp; Impact Analysis
                    </h3>
                    <ForecastChart data={forecast} />
                  </div>

                  <div className="bg-card border border-border rounded-lg p-4 h-[290px]">
                    <h3 className="text-xs font-bold uppercase text-muted-foreground mb-3">
                      Operational Efficiency Index
                    </h3>
                    <EfficiencyGauge value={efficiency} target={85} industry={78} />
                  </div>
                </div>
              </div>

              {/* AI insights cards */}
              <div className="bg-card border border-border rounded-lg p-4">
                <h3 className="text-xs font-bold uppercase text-muted-foreground mb-3">
                  Key Insights &amp; Recommendations
                </h3>
                <div className="grid md:grid-cols-3 gap-4">
                  {insights.map((ins, i) => (
                    <motion.div
                      key={i}
                      initial={{ opacity: 0, y: 20 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: i * 0.1 }}
                      className="bg-popover p-3 rounded-lg"
                    >
                      <div className="flex items-start gap-2">
                        <span className="text-lg">{ins.icon}</span>
                        <div>
                          <h4 className="text-sm font-semibold mb-1">{ins.title}</h4>
                          <p className="text-xs text-muted-foreground">{ins.description}</p>
                          <button className="text-xs text-primary mt-2 hover:underline">
                            {ins.action} →
                          </button>
                        </div>
                      </div>
                    </motion.div>
                  ))}
                </div>
              </div>
            </motion.div>
          )}

          {activeView === "analysis" && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="p-6"
            >
              <div className="bg-card border border-border rounded-lg p-6">
                <h2 className="text-lg font-semibold mb-4">Deep Analysis Mode</h2>
                {selectedAlert ? (
                  <>
                    <h3 className="text-sm font-medium mb-2">
                      Analyzing: {selectedAlert.title}
                    </h3>
                    <p className="text-sm text-muted-foreground">
                      {selectedAlert.description}
                    </p>
                    {/* extra analysis UI */}
                  </>
                ) : (
                  <p className="text-muted-foreground">
                    Select an alert to begin analysis
                  </p>
                )}
              </div>
            </motion.div>
          )}

          {activeView === "automation" && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="p-6"
            >
              <div className="bg-card border border-border rounded-lg p-6">
                <h2 className="text-lg font-semibold mb-6">
                  Automation Control Center
                </h2>
                <div className="grid md:grid-cols-3 gap-4">
                  {/* stubbed panels */}
                  {[
                    ["Marketing Automation", "3 campaigns ready to deploy"],
                    ["Supply Chain Automation", "2 route optimisations pending"],
                    ["Financial Automation", "All systems operational"],
                  ].map(([title, subtitle], i) => (
                    <div
                      key={i}
                      className="bg-popover rounded-lg p-4 flex flex-col"
                    >
                      <h3 className="text-sm font-medium mb-1">{title}</h3>
                      <p className="text-xs text-muted-foreground flex-1">
                        {subtitle}
                      </p>
                      <button className="btn-primary w-full text-xs mt-4">
                        Open
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            </motion.div>
          )}
        </div>

        {/* persistent ticker */}
        <NewsTicker />
      </section>

      {/* ───────── Right rail ───────── */}
      <aside className="hidden lg:flex flex-col border-l border-border overflow-y-auto">
        <MarketIntel data={marketData} />
      </aside>
    </Layouts>
  );
};

export default Dashboard;
