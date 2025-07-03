import React, { useState } from "react";
import { motion } from "framer-motion";
import Layouts from "../components/Layouts";

interface Opportunity {
  id: string;
  title: string;
  description: string;
  type: "market" | "product" | "operational" | "strategic";
  potential: number;
  confidence: number;
  timeframe: string;
  status: "new" | "analyzing" | "ready" | "implemented";
  actions: string[];
}

const Opportunities: React.FC = () => {
  const [filter, setFilter] = useState<"all" | "market" | "product" | "operational" | "strategic">("all");
  const [sortBy, setSortBy] = useState<"potential" | "confidence" | "timeframe">("potential");

  const opportunities: Opportunity[] = [
    {
      id: "1",
      title: "Perth Sustainable Gear Market",
      description: "340% increase in sustainable hiking gear searches. Untapped market with high growth potential.",
      type: "market",
      potential: 380000,
      confidence: 94,
      timeframe: "2-4 weeks",
      status: "ready",
      actions: ["Launch Campaign", "Analyze Demographics", "Partner with Influencers"],
    },
    {
      id: "2",
      title: "Bundle Strategy for Margin Protection",
      description: "Create product bundles to maintain margins while competing on perceived value.",
      type: "strategic",
      potential: 156000,
      confidence: 87,
      timeframe: "1-2 weeks",
      status: "ready",
      actions: ["Design Bundles", "Test Pricing", "Update Inventory"],
    },
    {
      id: "3",
      title: "Supply Chain Optimization",
      description: "Switch to regional suppliers to reduce lead times and transportation costs.",
      type: "operational",
      potential: 247000,
      confidence: 82,
      timeframe: "4-6 weeks",
      status: "analyzing",
      actions: ["Evaluate Suppliers", "Negotiate Terms", "Test Quality"],
    },
    {
      id: "4",
      title: "AI-Powered Inventory Management",
      description: "Implement predictive analytics to optimize stock levels and reduce carrying costs.",
      type: "operational",
      potential: 189000,
      confidence: 91,
      timeframe: "2-3 weeks",
      status: "new",
      actions: ["Select Platform", "Integrate Systems", "Train Team"],
    },
    {
      id: "5",
      title: "Premium Eco-Line Launch",
      description: "Develop high-margin sustainable product line targeting environmentally conscious consumers.",
      type: "product",
      potential: 520000,
      confidence: 78,
      timeframe: "8-12 weeks",
      status: "analyzing",
      actions: ["Product Development", "Market Research", "Launch Strategy"],
    },
  ];

  const getTypeColor = (type: Opportunity["type"]) => {
    switch (type) {
      case "market": return "bg-primary text-primary-foreground";
      case "product": return "bg-accent text-accent-foreground";
      case "operational": return "bg-warning text-warning-foreground";
      case "strategic": return "bg-destructive text-destructive-foreground";
    }
  };

  const getStatusColor = (status: Opportunity["status"]) => {
    switch (status) {
      case "new": return "text-muted-foreground";
      case "analyzing": return "text-warning";
      case "ready": return "text-accent";
      case "implemented": return "text-primary";
    }
  };

  const filteredOpportunities = opportunities
    .filter(opp => filter === "all" || opp.type === filter)
    .sort((a, b) => {
      switch (sortBy) {
        case "potential": return b.potential - a.potential;
        case "confidence": return b.confidence - a.confidence;
        case "timeframe": return a.timeframe.localeCompare(b.timeframe);
        default: return 0;
      }
    });

  return (
    <Layouts>
      <div className="container mx-auto px-6 py-6">
        <div className="mb-6">
          <h1 className="text-2xl font-bold mb-2">Opportunities</h1>
          <p className="text-muted-foreground">
            AI-identified opportunities for growth and optimization
          </p>
        </div>

        {/* Filters and Controls */}
        <div className="flex flex-wrap items-center justify-between gap-4 mb-6">
          <div className="flex items-center gap-2">
            {["all", "market", "product", "operational", "strategic"].map((type) => (
              <button
                key={type}
                onClick={() => setFilter(type as any)}
                className={`px-3 py-1.5 text-xs font-medium rounded transition-all ${
                  filter === type
                    ? "bg-primary text-primary-foreground"
                    : "text-muted-foreground hover:text-foreground hover:bg-popover"
                }`}
              >
                {type.toUpperCase()}
              </button>
            ))}
          </div>

          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as any)}
            className="text-xs bg-popover border border-border rounded px-3 py-1.5"
          >
            <option value="potential">Sort by Potential</option>
            <option value="confidence">Sort by Confidence</option>
            <option value="timeframe">Sort by Timeframe</option>
          </select>
        </div>

        {/* Summary Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <div className="bg-card rounded-lg border border-border p-4">
            <div className="text-2xl font-bold text-accent">
              ${(filteredOpportunities.reduce((sum, opp) => sum + opp.potential, 0) / 1000).toFixed(0)}K
            </div>
            <div className="text-xs text-muted-foreground">Total Potential Value</div>
          </div>
          <div className="bg-card rounded-lg border border-border p-4">
            <div className="text-2xl font-bold text-primary">
              {filteredOpportunities.filter(opp => opp.status === "ready").length}
            </div>
            <div className="text-xs text-muted-foreground">Ready to Implement</div>
          </div>
          <div className="bg-card rounded-lg border border-border p-4">
            <div className="text-2xl font-bold text-warning">
              {Math.round(filteredOpportunities.reduce((sum, opp) => sum + opp.confidence, 0) / filteredOpportunities.length)}%
            </div>
            <div className="text-xs text-muted-foreground">Average Confidence</div>
          </div>
          <div className="bg-card rounded-lg border border-border p-4">
            <div className="text-2xl font-bold text-destructive">
              {filteredOpportunities.filter(opp => opp.confidence > 85).length}
            </div>
            <div className="text-xs text-muted-foreground">High Confidence</div>
          </div>
        </div>

        {/* Opportunities Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {filteredOpportunities.map((opportunity, index) => (
            <motion.div
              key={opportunity.id}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.05 }}
              className="bg-card rounded-lg border border-border p-6 hover:border-primary transition-all cursor-pointer"
            >
              <div className="flex items-start justify-between mb-4">
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <span className={`px-2 py-1 text-[10px] font-medium rounded ${getTypeColor(opportunity.type)}`}>
                      {opportunity.type.toUpperCase()}
                    </span>
                    <span className={`text-xs font-medium ${getStatusColor(opportunity.status)}`}>
                      {opportunity.status.toUpperCase()}
                    </span>
                  </div>
                  <h3 className="text-lg font-semibold">{opportunity.title}</h3>
                </div>
                <div className="text-right">
                  <div className="text-2xl font-bold text-accent">
                    ${(opportunity.potential / 1000).toFixed(0)}K
                  </div>
                  <div className="text-xs text-muted-foreground">potential</div>
                </div>
              </div>

              <p className="text-sm text-muted-foreground mb-4">
                {opportunity.description}
              </p>

              <div className="grid grid-cols-2 gap-4 mb-4">
                <div>
                  <div className="text-xs text-muted-foreground mb-1">Confidence</div>
                  <div className="flex items-center gap-2">
                    <div className="flex-1 h-2 bg-popover rounded-full overflow-hidden">
                      <motion.div
                        initial={{ width: 0 }}
                        animate={{ width: `${opportunity.confidence}%` }}
                        transition={{ delay: 0.5 + index * 0.05 }}
                        className="h-full bg-primary"
                      />
                    </div>
                    <span className="text-xs font-medium">{opportunity.confidence}%</span>
                  </div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground mb-1">Timeframe</div>
                  <div className="text-sm font-medium">{opportunity.timeframe}</div>
                </div>
              </div>

              <div className="flex flex-wrap gap-2">
                {opportunity.actions.map((action, i) => (
                  <button
                    key={i}
                    className="px-3 py-1 text-xs bg-popover hover:bg-primary hover:text-primary-foreground rounded transition-all"
                  >
                    {action}
                  </button>
                ))}
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </Layouts>
  );
};

export default Opportunities;