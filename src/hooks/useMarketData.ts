import { useQuery } from "@tanstack/react-query";
import { wsURL } from "./api";
import { useEffect } from "react";

interface MarketDataItem {
  id: string;
  type: "competitor" | "trend" | "risk" | "opportunity";
  title: string;
  description: string;
  impact: "high" | "medium" | "low";
  timestamp: string;
  source: string;
  confidence: number;
}

export const useMarketData = () => {
  const query = useQuery({
    queryKey: ["market-data"],
    queryFn: async (): Promise<MarketDataItem[]> => {
      // In production, this would fetch from your API
      // For now, return demo data
      return [
        {
          id: "1",
          type: "competitor",
          title: "Aussie Gear Price Cut",
          description: "20% discount on waterproof jackets across NSW stores",
          impact: "high",
          timestamp: new Date().toISOString(),
          source: "Price Monitor",
          confidence: 95,
        },
        {
          id: "2",
          type: "trend",
          title: "Sustainable Gear Surge",
          description: "340% increase in eco-friendly product searches in Perth",
          impact: "high",
          timestamp: new Date().toISOString(),
          source: "Google Trends",
          confidence: 88,
        },
        {
          id: "3",
          type: "risk",
          title: "Supply Chain Alert",
          description: "Shipping delays expected via Malacca Strait",
          impact: "medium",
          timestamp: new Date().toISOString(),
          source: "Logistics API",
          confidence: 75,
        },
      ];
    },
    refetchInterval: 60000, // Refresh every minute
  });

  // WebSocket for real-time updates
  useEffect(() => {
    const ws = new WebSocket(wsURL("/ws/market"));
    
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      // Update cache with new market data
      query.refetch();
    };

    return () => ws.close();
  }, []);

  return query;
};