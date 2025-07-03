import React, { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Alert } from "../../types";
import { wsURL } from "../../hooks/api";

interface AlertStreamProps {
  alerts?: Alert[];
  onAction?: (action: string, alertId: string) => void;
}

const AlertStream: React.FC<AlertStreamProps> = ({ alerts: initialAlerts = [], onAction }) => {
  const [alerts, setAlerts] = useState<Alert[]>(initialAlerts);
  const [flashingAlerts, setFlashingAlerts] = useState<Set<string>>(new Set());

  // WebSocket connection for live updates
  useEffect(() => {
    const ws = new WebSocket(wsURL("/ws/alerts"));
    
    ws.onmessage = (event) => {
      const newAlert: Alert = JSON.parse(event.data);
      setAlerts(prev => [newAlert, ...prev.slice(0, 49)]);
      
      // Flash new alert
      setFlashingAlerts(prev => new Set(prev).add(newAlert.id));
      setTimeout(() => {
        setFlashingAlerts(prev => {
          const next = new Set(prev);
          next.delete(newAlert.id);
          return next;
        });
      }, 500);
    };

    return () => ws.close();
  }, []);

  const getSeverityClass = (severity: Alert['severity']) => {
    switch (severity) {
      case 'critical': return 'severity-critical';
      case 'warning': return 'severity-warning';
      case 'info': return 'severity-info';
    }
  };

  const getActionStyle = (style: string) => {
    switch (style) {
      case 'primary': return 'btn-primary';
      case 'danger': return 'btn-danger';
      case 'warning': return 'btn-warning';
      default: return 'btn-ghost';
    }
  };

  const formatTime = (timestamp: string) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit',
      hour12: false 
    });
  };

  return (
    <section className="h-full flex flex-col bg-card">
      <header className="flex items-center justify-between px-4 py-2 border-b border-border">
        <h2 className="text-xs font-bold uppercase tracking-wider text-muted-foreground">
          Priority Alerts
        </h2>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground">
            {alerts.length} ACTIVE
          </span>
          <span className="live-dot" />
          <span className="text-[10px] text-accent">LIVE</span>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto">
        <AnimatePresence mode="popLayout">
          {alerts.map((alert, index) => (
            <motion.div
              key={alert.id}
              layout
              initial={{ x: -20, opacity: 0 }}
              animate={{ x: 0, opacity: 1 }}
              exit={{ x: 20, opacity: 0 }}
              transition={{ 
                type: "spring",
                stiffness: 500,
                damping: 30,
                delay: index * 0.05 
              }}
              className={`
                p-3 border-b border-border cursor-pointer
                hover:bg-popover transition-colors
                ${flashingAlerts.has(alert.id) ? 'flash' : ''}
              `}
            >
              <div className="flex items-start gap-2">
                <div className={`severity-dot ${getSeverityClass(alert.severity)} mt-1`} />
                
                <div className="flex-1 min-w-0">
                  <div className="flex items-center justify-between mb-1">
                    <h3 className="text-[11px] font-semibold text-foreground uppercase">
                      {alert.title}
                    </h3>
                    <span className="text-[9px] text-muted-foreground font-mono">
                      {formatTime(alert.timestamp)}
                    </span>
                  </div>
                  
                  <p className="text-[10px] text-muted-foreground mb-2 leading-relaxed">
                    {alert.description}
                  </p>
                  
                  {/* Metrics */}
                  {alert.metrics.length > 0 && (
                    <div className="flex flex-wrap gap-3 mb-2 text-[10px]">
                      {alert.metrics.map((metric, i) => (
                        <span key={i} className="text-warning font-medium">
                          {metric.label}: {metric.value}
                          {metric.trend && (
                            <span className={metric.trend === 'up' ? 'text-destructive' : 'text-accent'}>
                              {' '}
                              {metric.trend === 'up' ? '↑' : '↓'}
                            </span>
                          )}
                        </span>
                      ))}
                    </div>
                  )}

                  {/* Actions */}
                  {alert.actions && alert.actions.length > 0 && (
                    <div className="flex flex-wrap gap-1">
                      {alert.actions.map((action) => (
                        <button
                          key={action.id}
                          onClick={(e) => {
                            e.stopPropagation();
                            onAction?.(action.type, alert.id);
                          }}
                          className={`
                            ${getActionStyle(action.style)}
                            ${action.automated ? 'font-semibold' : ''}
                            relative overflow-hidden
                          `}
                        >
                          {action.automated && (
                            <span className="absolute inset-0 bg-gradient-to-r from-transparent via-white/10 to-transparent animate-shimmer" />
                          )}
                          <span className="relative">
                            {action.icon && <span className="mr-1">{action.icon}</span>}
                            {action.label}
                          </span>
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </motion.div>
          ))}
        </AnimatePresence>

        {alerts.length === 0 && (
          <div className="flex items-center justify-center h-32 text-muted-foreground text-xs">
            <p>No alerts at this time</p>
          </div>
        )}
      </div>
    </section>
  );
};

export default AlertStream;