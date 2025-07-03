import React, { useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface MarketIntelItem {
  id: string;
  type: "competitor" | "trend" | "risk" | "opportunity";
  title: string;
  description: string;
  impact: "high" | "medium" | "low";
  timestamp: string;
  source: string;
  confidence: number;
}

interface MarketIntelProps {
  data?: MarketIntelItem[];
}

const MarketIntel: React.FC<MarketIntelProps> = ({ data }) => {
  const [items, setItems] = useState<MarketIntelItem[]>(data || []);
  const [filter, setFilter] = useState<"all" | "competitor" | "trend" | "risk" | "opportunity">("all");

  // Demo data if none provided
  useEffect(() => {
    if (!data || data.length === 0) {
      setItems([
        {
          id: "1",
          type: "competitor",
          title: "Aussie Gear Price Cut",
          description: "20% discount on waterproof jackets across NSW stores",
          impact: "high",
          timestamp: "2 min ago",
          source: "Price Monitor",
          confidence: 95,
        },
        {
          id: "2",
          type: "trend",
          title: "Sustainable Gear Surge",
          description: "340% increase in eco-friendly product searches in Perth",
          impact: "high",
          timestamp: "15 min ago",
          source: "Google Trends",
          confidence: 88,
        },
        {
          id: "3",
          type: "risk",
          title: "Supply Chain Alert",
          description: "Shipping delays expected via Malacca Strait",
          impact: "medium",
          timestamp: "1 hour ago",
          source: "Logistics API",
          confidence: 75,
        },
        {
          id: "4",
          type: "opportunity",
          title: "Market Gap Identified",
          description: "Unmet demand for lightweight camping gear in QLD",
          impact: "medium",
          timestamp: "2 hours ago",
          source: "AI Analysis",
          confidence: 82,
        },
      ]);
    }
  }, [data]);

  const getTypeIcon = (type: MarketIntelItem["type"]) => {
    switch (type) {
      case "competitor": return "🏪";
      case "trend": return "📈";
      case "risk": return "⚠️";
      case "opportunity": return "💡";
    }
  };

  const getTypeColor = (type: MarketIntelItem["type"]) => {
    switch (type) {
      case "competitor": return "text-destructive";
      case "trend": return "text-primary";
      case "risk": return "text-warning";
      case "opportunity": return "text-accent";
    }
  };

  const getImpactColor = (impact: MarketIntelItem["impact"]) => {
    switch (impact) {
      case "high": return "bg-destructive";
      case "medium": return "bg-warning";
      case "low": return "bg-primary";
    }
  };

  const filteredItems = filter === "all" ? items : items.filter(item => item.type === filter);

  return (
    <div className="h-full flex flex-col">
      <div className="p-4 border-b border-border">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-xs font-bold uppercase text-muted-foreground">
            Market Intelligence
          </h2>
          <span className="text-[10px] text-accent flex items-center gap-1">
            <span className="live-dot" />
            LIVE
          </span>
        </div>

        {/* Filter tabs */}
        <div className="flex gap-1">
          {["all", "competitor", "trend", "risk", "opportunity"].map((type) => (
            <button
              key={type}
              onClick={() => setFilter(type as any)}
              className={`px-2 py-1 text-[10px] rounded transition-all ${
                filter === type
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              {type.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-2 space-y-2">
        <AnimatePresence mode="popLayout">
          {filteredItems.map((item, index) => (
            <motion.div
              key={item.id}
              layout
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -20 }}
              transition={{ delay: index * 0.05 }}
              className="p-3 bg-popover rounded-lg border border-border/50 hover:border-border transition-all cursor-pointer group"
            >
              <div className="flex items-start gap-2">
                <span className="text-lg">{getTypeIcon(item.type)}</span>
                <div className="flex-1 min-w-0">
                  <div className="flex items-start justify-between gap-2 mb-1">
                    <h3 className={`text-xs font-semibold ${getTypeColor(item.type)}`}>
                      {item.title}
                    </h3>
                    <span className={`px-1.5 py-0.5 text-[9px] rounded ${getImpactColor(item.impact)} text-white`}>
                      {item.impact.toUpperCase()}
                    </span>
                  </div>
                  <p className="text-[10px] text-muted-foreground mb-2">
                    {item.description}
                  </p>
                  <div className="flex items-center justify-between text-[9px]">
                    <div className="flex items-center gap-2 text-muted-foreground">
                      <span>{item.source}</span>
                      <span>•</span>
                      <span>{item.confidence}% confidence</span>
                    </div>
                    <span className="text-muted-foreground">{item.timestamp}</span>
                  </div>

                  {/* Confidence bar */}
                  <div className="mt-2 h-1 bg-muted rounded-full overflow-hidden">
                    <motion.div
                      initial={{ width: 0 }}
                      animate={{ width: `${item.confidence}%` }}
                      transition={{ delay: 0.2 + index * 0.05 }}
                      className="h-full bg-primary"
                    />
                  </div>
                </div>
              </div>

              {/* Quick action on hover */}
              <motion.div
                initial={{ opacity: 0, height: 0 }}
                animate={{ opacity: 0, height: 0 }}
                whileHover={{ opacity: 1, height: "auto" }}
                className="mt-2 pt-2 border-t border-border/50"
              >
                <button className="text-[10px] text-primary hover:underline">
                  Analyze Impact →
                </button>
              </motion.div>
            </motion.div>
          ))}
        </AnimatePresence>
      </div>

      {/* Summary stats */}
      <div className="p-3 border-t border-border bg-card/50">
        <div className="grid grid-cols-2 gap-2 text-[10px]">
          <div>
            <span className="text-muted-foreground">High Impact:</span>
            <span className="ml-1 font-semibold text-destructive">
              {items.filter(i => i.impact === "high").length}
            </span>
          </div>
          <div>
            <span className="text-muted-foreground">Opportunities:</span>
            <span className="ml-1 font-semibold text-accent">
              {items.filter(i => i.type === "opportunity").length}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default MarketIntel;