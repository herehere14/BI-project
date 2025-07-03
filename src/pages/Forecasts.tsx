import React, { useState } from "react";
import { useLocation } from "react-router-dom";
import { motion } from "framer-motion";
import Layouts from "../components/Layouts";
import ForecastChart from "../components/dashboard/ForecastChart";

const Forecasts: React.FC = () => {
  const location = useLocation();
  const [selectedMetric, setSelectedMetric] = useState("revenue");
  const [timeRange, setTimeRange] = useState("30d");
  const [scenario, setScenario] = useState("baseline");

  const metrics = [
    { id: "revenue", label: "Revenue", icon: "💰" },
    { id: "margin", label: "Gross Margin", icon: "📊" },
    { id: "inventory", label: "Inventory Turn", icon: "📦" },
    { id: "cash", label: "Cash Flow", icon: "💵" },
  ];

  const scenarios = [
    { id: "baseline", label: "Baseline", description: "Current trajectory" },
    { id: "optimistic", label: "Optimistic", description: "Best case scenario" },
    { id: "pessimistic", label: "Pessimistic", description: "Worst case scenario" },
    { id: "custom", label: "Custom", description: "User defined parameters" },
  ];

  return (
    <Layouts>
      <div className="container mx-auto px-6 py-6">
        <div className="mb-6">
          <h1 className="text-2xl font-bold mb-2">Forecasts & Scenarios</h1>
          <p className="text-muted-foreground">
            Model future outcomes and plan for various scenarios
          </p>
        </div>

        {/* Controls */}
        <div className="grid grid-cols-12 gap-6 mb-6">
          <div className="col-span-12 lg:col-span-3">
            <div className="bg-card rounded-lg border border-border p-4">
              <h3 className="text-sm font-semibold mb-3">Select Metric</h3>
              <div className="space-y-2">
                {metrics.map((metric) => (
                  <button
                    key={metric.id}
                    onClick={() => setSelectedMetric(metric.id)}
                    className={`w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-all ${
                      selectedMetric === metric.id
                        ? "bg-primary text-primary-foreground"
                        : "hover:bg-popover"
                    }`}
                  >
                    <span>{metric.icon}</span>
                    <span>{metric.label}</span>
                  </button>
                ))}
              </div>
            </div>
          </div>

          <div className="col-span-12 lg:col-span-6">
            <div className="bg-card rounded-lg border border-border p-4 h-[400px]">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-sm font-semibold">Forecast Visualization</h3>
                <select
                  value={timeRange}
                  onChange={(e) => setTimeRange(e.target.value)}
                  className="text-xs bg-popover border border-border rounded px-2 py-1"
                >
                  <option value="7d">7 Days</option>
                  <option value="30d">30 Days</option>
                  <option value="90d">90 Days</option>
                  <option value="1y">1 Year</option>
                </select>
              </div>
              <ForecastChart />
            </div>
          </div>

          <div className="col-span-12 lg:col-span-3">
            <div className="bg-card rounded-lg border border-border p-4">
              <h3 className="text-sm font-semibold mb-3">Scenarios</h3>
              <div className="space-y-2">
                {scenarios.map((item) => (
                  <button
                    key={item.id}
                    onClick={() => setScenario(item.id)}
                    className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-all ${
                      scenario === item.id
                        ? "bg-primary text-primary-foreground"
                        : "hover:bg-popover"
                    }`}
                  >
                    <div className="font-medium">{item.label}</div>
                    <div className="text-xs opacity-80">{item.description}</div>
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Insights */}
        <div className="grid grid-cols-12 gap-6">
          <div className="col-span-12">
            <div className="bg-card rounded-lg border border-border p-6">
              <h3 className="text-lg font-semibold mb-4">AI-Generated Insights</h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  className="p-4 bg-popover rounded-lg"
                >
                  <div className="flex items-start gap-3">
                    <span className="text-2xl">📈</span>
                    <div>
                      <h4 className="text-sm font-semibold mb-1">Growth Opportunity</h4>
                      <p className="text-xs text-muted-foreground">
                        Revenue could increase by 23% if Perth campaign is launched within 2 weeks
                      </p>
                      <button className="text-xs text-primary mt-2 hover:underline">
                        View Campaign Plan →
                      </button>
                    </div>
                  </div>
                </motion.div>

                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.1 }}
                  className="p-4 bg-popover rounded-lg"
                >
                  <div className="flex items-start gap-3">
                    <span className="text-2xl">⚠️</span>
                    <div>
                      <h4 className="text-sm font-semibold mb-1">Risk Alert</h4>
                      <p className="text-xs text-muted-foreground">
                        Supply chain disruption could impact Q3 revenue by -$156K
                      </p>
                      <button className="text-xs text-primary mt-2 hover:underline">
                        Mitigation Options →
                      </button>
                    </div>
                  </div>
                </motion.div>

                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.2 }}
                  className="p-4 bg-popover rounded-lg"
                >
                  <div className="flex items-start gap-3">
                    <span className="text-2xl">💡</span>
                    <div>
                      <h4 className="text-sm font-semibold mb-1">Optimization</h4>
                      <p className="text-xs text-muted-foreground">
                        Adjusting inventory levels could free up $247K in working capital
                      </p>
                      <button className="text-xs text-primary mt-2 hover:underline">
                        Implement Changes →
                      </button>
                    </div>
                  </div>
                </motion.div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Layouts>
  );
};

export default Forecasts;