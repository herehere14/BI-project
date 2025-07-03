import React, { useEffect, useState } from "react";
import { motion } from "framer-motion";
import { Metric } from "../../types";
import MiniSparkline from "./MiniSparkline";

interface MetricsBarProps {
  metrics?: Metric[];
  isLoading?: boolean;
}

const MetricsBar: React.FC<MetricsBarProps> = ({ metrics = [], isLoading }) => {
  const [flashingMetrics, setFlashingMetrics] = useState<Set<string>>(new Set());

  // Simulate real-time updates with flashing
  useEffect(() => {
    const interval = setInterval(() => {
      const randomIndex = Math.floor(Math.random() * metrics.length);
      const metric = metrics[randomIndex];
      if (metric) {
        setFlashingMetrics(new Set([metric.id]));
        setTimeout(() => setFlashingMetrics(new Set()), 500);
      }
    }, 5000);

    return () => clearInterval(interval);
  }, [metrics]);

  const getTrendColor = (metric: Metric) => {
    if (metric.status === 'critical') return 'text-destructive';
    if (metric.status === 'warning') return 'text-warning';
    
    // For metrics where up is good (revenue, NPS, etc)
    if (['revenue', 'nps', 'margin', 'inventory_turn'].includes(metric.id)) {
      return metric.change > 0 ? 'text-accent' : 'text-destructive';
    }
    
    // For metrics where down is good (costs, risk, etc)
    if (['supply_risk', 'costs'].includes(metric.id)) {
      return metric.change < 0 ? 'text-accent' : 'text-destructive';
    }
    
    return 'text-muted-foreground';
  };

  const getTrendIcon = (trend: string) => {
    switch (trend) {
      case 'up': return '▲';
      case 'down': return '▼';
      default: return '—';
    }
  };

  const getChartColor = (metric: Metric) => {
    if (metric.status === 'critical') return 'rgb(255 59 59)';
    if (metric.status === 'warning') return 'rgb(255 184 0)';
    if (metric.change > 0) return 'rgb(0 255 136)';
    return 'rgb(255 184 0)';
  };

  if (isLoading) {
    return (
      <div className="data-grid grid-cols-8">
        {[...Array(8)].map((_, i) => (
          <div key={i} className="data-cell animate-pulse">
            <div className="h-3 bg-muted rounded w-20 mb-2" />
            <div className="h-6 bg-muted rounded w-16 mb-2" />
            <div className="h-3 bg-muted rounded w-12" />
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="data-grid grid-cols-4 lg:grid-cols-8">
      {metrics.map((metric, index) => (
        <motion.div
          key={metric.id}
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: index * 0.05 }}
          className={`
            metric-card relative overflow-hidden
            ${flashingMetrics.has(metric.id) ? 'flash' : ''}
          `}
        >
          {/* Status indicator */}
          {metric.status && (
            <div className={`
              absolute top-0 right-0 w-8 h-8 -mr-4 -mt-4
              ${metric.status === 'critical' ? 'bg-destructive/20' : 
                metric.status === 'warning' ? 'bg-warning/20' : 'bg-accent/20'}
              rounded-full blur-xl
            `} />
          )}

          {/* Metric label */}
          <div className="text-[9px] text-muted-foreground uppercase tracking-wider mb-1 font-medium">
            {metric.label}
          </div>
          
          {/* Metric value */}
          <div className={`
            text-lg font-bold mb-1 metric-value
            ${metric.status === 'critical' ? 'text-destructive' : 
              metric.status === 'warning' ? 'text-warning' : 
              'text-foreground'}
          `}>
            {metric.value}{metric.unit}
          </div>
          
          {/* Change indicator */}
          <div className="flex items-center gap-1 mb-2 text-[10px]">
            <span className={getTrendColor(metric)}>
              {getTrendIcon(metric.trend)} 
              {Math.abs(metric.change)}
              {metric.changeType === 'percentage' ? '%' : 
               metric.changeType === 'multiplier' ? 'x' : ''}
            </span>
            {metric.subtitle && (
              <span className="text-muted-foreground text-[9px]">
                {metric.subtitle}
              </span>
            )}
          </div>
          
          {/* Mini sparkline */}
          <div className="h-5 -mx-1">
            <MiniSparkline 
              data={metric.sparkline} 
              color={getChartColor(metric)}
              showArea={metric.status === 'critical' || metric.status === 'warning'}
            />
          </div>

          {/* Special indicators */}
          {metric.id === 'cash' && (
            <div className="absolute bottom-1 right-1 text-[8px] text-muted-foreground">
              14.2 MO RUNWAY
            </div>
          )}
          
          {metric.id === 'supply_risk' && (
            <div className="flex gap-0.5 mt-1">
              {[...Array(5)].map((_, i) => (
                <div
                  key={i}
                  className={`h-1 flex-1 rounded-full ${
                    i < 3 ? 'bg-destructive' : 'bg-muted'
                  }`}
                />
              ))}
            </div>
          )}
        </motion.div>
      ))}
    </div>
  );
};

export default MetricsBar;