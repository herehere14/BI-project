import { useQuery } from "@tanstack/react-query";
import { fetchDashboard, fetchAlerts } from "./api";
import { 
  DashboardData, 
  Metric, 
  Alert, 
  KPITile,
  AlertMetric,  // Add this
  AlertAction   // Add this
} from "../types";

export const useDashboardData = () => {
  const { data, isLoading, error } = useQuery({
    queryKey: ["dashboard"],
    queryFn: async (): Promise<DashboardData> => {
      try {
        const [dashboardResponse, alertsResponse] = await Promise.all([
          fetchDashboard(),
          fetchAlerts()
        ]);

        // Transform KPITile data to Metric format
        const metrics: Metric[] = dashboardResponse.map((kpi: KPITile) => ({
          id: kpi.label.toLowerCase().replace(/\s+/g, '_'),
          label: kpi.label.toUpperCase(),
          value: kpi.value,
          change: kpi.delta_pct,
          changeType: 'percentage' as const,
          trend: kpi.delta_pct > 0 ? 'up' as const : kpi.delta_pct < 0 ? 'down' as const : 'neutral' as const,
          sparkline: kpi.spark || generateSparkline(),
          unit: kpi.unit,
          status: getMetricStatus(kpi),
          subtitle: 'vs LM'
        }));

        // Transform alerts to include all required fields
        const alerts: Alert[] = alertsResponse.map((alert: Alert) => ({
          id: alert.id || `alert-${Date.now()}-${Math.random()}`,
          ts: alert.ts,
          severity: alert.severity || 'info',
          headline: alert.headline,
          title: alert.headline, // Use headline as title if not provided
          description: alert.details || alert.headline,
          kpi: alert.kpi,
          suggested_action: alert.suggested_action,
          metrics: parseAlertMetrics(alert),
          actions: generateAlertActions(alert),
          category: categorizeAlert(alert)
        }));

        // Add demo data to match what Dashboard expects
        const dashboardData: DashboardData = {
          metrics,
          alerts,
          marketData: [],
          forecast: {
            dates: Array.from({ length: 30 }, (_, i) => {
              const date = new Date();
              date.setDate(date.getDate() + i);
              return date.toISOString();
            }),
            baseline: Array.from({ length: 30 }, () => 3000 + Math.random() * 500),
            forecast: Array.from({ length: 30 }, () => 2800 + Math.random() * 500),
            lower: Array.from({ length: 30 }, () => 2600 + Math.random() * 500),
            upper: Array.from({ length: 30 }, () => 3000 + Math.random() * 500),
          },
          efficiency: 78,
          insights: [
            {
              icon: "📈",
              title: "Growth Opportunity",
              description: "Perth sustainable gear market shows 340% search growth",
              action: "Launch Campaign"
            },
            {
              icon: "⚠️",
              title: "Supply Risk",
              description: "Shipping delays may impact Q3 inventory",
              action: "Find Alternatives"
            },
            {
              icon: "💡",
              title: "Cost Optimization",
              description: "Bundle strategy could protect margins by 2.3%",
              action: "Implement Strategy"
            }
          ],
          lastUpdated: new Date().toISOString()
        };

        return dashboardData;
      } catch (error) {
        console.error("Error fetching dashboard data:", error);
        // Return empty data structure on error
        return {
          metrics: [],
          alerts: [],
          marketData: [],
          lastUpdated: new Date().toISOString()
        };
      }
    },
    refetchInterval: 30000,
    staleTime: 10000,
  });

  return {
    data,
    isLoading,
    error,
  };
};

// Helper functions
function generateSparkline(points = 7): number[] {
  const baseline = 50;
  return Array.from({ length: points }, (_, i) => {
    const trend = i * 2;
    const noise = Math.random() * 20 - 10;
    return Math.max(0, Math.min(100, baseline + trend + noise));
  });
}

function getMetricStatus(kpi: KPITile): 'good' | 'warning' | 'critical' | undefined {
  const label = kpi.label.toLowerCase();
  
  if (label.includes('supply') && label.includes('risk')) return 'critical';
  if (label.includes('margin') && kpi.delta_pct < -1) return 'warning';
  if (label.includes('revenue') && kpi.delta_pct > 10) return 'good';
  if (label.includes('share') && kpi.delta_pct < -1) return 'critical';
  if (label.includes('nps') && kpi.value > 70) return 'good';
  if (label.includes('cash') && kpi.value < 5) return 'warning';
  
  return undefined;
}

function parseAlertMetrics(alert: Alert): AlertMetric[] {
  const metrics: AlertMetric[] = [];
  
  // Extract metrics from headline
  const moneyMatch = alert.headline?.match(/\$?[\d,]+K?M?/);
  if (moneyMatch) {
    metrics.push({
      label: 'IMPACT',
      value: moneyMatch[0].startsWith('$') ? moneyMatch[0] : `$${moneyMatch[0]}`
    });
  }
  
  const percentMatch = alert.headline?.match(/([\d.]+)%/);
  if (percentMatch) {
    metrics.push({
      label: 'CHANGE',
      value: percentMatch[0],
      trend: alert.headline.toLowerCase().includes('down') ? 'down' : 'up'
    });
  }
  
  if (alert.kpi) {
    metrics.push({
      label: 'KPI',
      value: alert.kpi
    });
  }
  
  return metrics;
}

function generateAlertActions(alert: Alert): AlertAction[] {
  const actions: AlertAction[] = [];
  
  // Add suggested action if available
  if (alert.suggested_action) {
    actions.push({
      id: 'suggested',
      label: alert.suggested_action,
      type: 'custom',
      style: 'primary'
    });
  }
  
  // Add automated actions based on alert content
  const headline = alert.headline?.toLowerCase() || '';
  
  if (headline.includes('sales') || headline.includes('revenue') || headline.includes('customer')) {
    actions.push({
      id: 'marketing',
      label: '🎯 AUTO-MARKETING',
      icon: '🎯',
      type: 'marketing',
      style: 'primary',
      automated: true
    });
  }
  
  if (headline.includes('supply') || headline.includes('shipment') || headline.includes('inventory')) {
    actions.push({
      id: 'logistics',
      label: '🚚 AUTO-LOGISTICS',
      icon: '🚚',
      type: 'logistics',
      style: 'warning',
      automated: true
    });
  }
  
  if (headline.includes('competitor') || headline.includes('price')) {
    actions.push({
      id: 'counter',
      label: '⚡ COUNTER-STRIKE',
      icon: '⚡',
      type: 'analysis',
      style: 'danger',
      automated: true
    });
  }
  
  // Always add analyze option
  actions.push({
    id: 'analyze',
    label: '📊 Analyze',
    type: 'analysis',
    style: 'default'
  });
  
  return actions;
}

function categorizeAlert(alert: Alert): Alert['category'] {
  const headline = alert.headline?.toLowerCase() || '';
  
  if (headline.includes('sales') || headline.includes('revenue') || headline.includes('customer')) {
    return 'sales';
  }
  if (headline.includes('supply') || headline.includes('shipment') || headline.includes('inventory')) {
    return 'supply';
  }
  if (headline.includes('competitor') || headline.includes('market share')) {
    return 'competitor';
  }
  if (headline.includes('market') || headline.includes('trend')) {
    return 'market';
  }
  if (headline.includes('margin') || headline.includes('cost') || headline.includes('cash')) {
    return 'finance';
  }
  
  return 'operations';
}