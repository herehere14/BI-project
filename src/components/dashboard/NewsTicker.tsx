import React from "react";

const NewsTicker: React.FC = () => {
  const tickerItems = [
    { label: "ASX 200", value: "7,421.50", change: "▼0.3%", color: "text-destructive" },
    { label: "AUD/USD", value: "0.6542", change: "▲0.1%", color: "text-accent" },
    { label: "WEATHER", value: "SYD 18°C Clear | MEL 14°C Rain | BNE 24°C Sunny", change: "", color: "text-foreground" },
    { label: "SUPPLY ALERTS", value: "3 Active", change: "", color: "text-warning" },
    { label: "COMPETITOR MOVES", value: "2 Today", change: "", color: "text-destructive" },
    { label: "OPPORTUNITIES", value: "5 Identified", change: "", color: "text-accent" },
    { label: "INVENTORY", value: "1,247 SKUs", change: "", color: "text-foreground" },
    { label: "LOW STOCK", value: "17 Items", change: "", color: "text-warning" },
    { label: "CUSTOMER ALERTS", value: "VIP Activity ▲23%", change: "", color: "text-accent" },
    { label: "NPS TREND", value: "POSITIVE", change: "", color: "text-accent" },
    { label: "CASH RUNWAY", value: "14.2 Months", change: "", color: "text-primary" },
    { label: "AI CONFIDENCE", value: "94.2%", change: "▲2.1%", color: "text-accent" },
  ];

  return (
    <div className="fixed bottom-0 left-0 right-0 h-6 bg-background/95 backdrop-blur border-t border-border overflow-hidden z-50">
      <div className="flex h-full items-center">
        <div className="ticker-content">
          {/* First set */}
          {tickerItems.map((item, index) => (
            <span key={`first-${index}`} className="inline-flex items-center gap-1 text-xs mx-4">
              <span className="font-semibold text-muted-foreground">{item.label}:</span>
              <span className={item.color}>{item.value}</span>
              {item.change && <span className={item.color}>{item.change}</span>}
            </span>
          ))}
          
          {/* Duplicate for seamless loop */}
          {tickerItems.map((item, index) => (
            <span key={`second-${index}`} className="inline-flex items-center gap-1 text-xs mx-4">
              <span className="font-semibold text-muted-foreground">{item.label}:</span>
              <span className={item.color}>{item.value}</span>
              {item.change && <span className={item.color}>{item.change}</span>}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
};

export default NewsTicker;