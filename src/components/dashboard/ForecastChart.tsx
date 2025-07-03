import React from "react";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

interface ForecastChartProps {
  data?: {
    dates: string[];
    baseline: number[];
    forecast: number[];
    lower: number[];
    upper: number[];
  };
}

const ForecastChart: React.FC<ForecastChartProps> = ({ data }) => {
  if (!data) {
    // Generate demo data
    const dates = Array.from({ length: 30 }, (_, i) => {
      const date = new Date();
      date.setDate(date.getDate() + i);
      return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
    });
    
    const baseline = Array.from({ length: 30 }, (_, i) => 
      3000 + Math.random() * 500 + i * 20
    );
    
    data = {
      dates,
      baseline,
      forecast: baseline.map((v, i) => v - (i > 15 ? 200 + Math.random() * 100 : 0)),
      lower: baseline.map((v, i) => v - (i > 15 ? 300 + Math.random() * 100 : 0)),
      upper: baseline.map((v, i) => v - (i > 15 ? 100 + Math.random() * 100 : 0)),
    };
  }

  const chartData = {
    labels: data.dates,
    datasets: [
      {
        label: "Baseline",
        data: data.baseline,
        borderColor: "rgb(0, 163, 255)",
        backgroundColor: "transparent",
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.4,
      },
      {
        label: "Forecast",
        data: data.forecast,
        borderColor: "rgb(255, 59, 59)",
        backgroundColor: "transparent",
        borderWidth: 2,
        borderDash: [5, 5],
        pointRadius: 0,
        tension: 0.4,
      },
      {
        label: "Upper Bound",
        data: data.upper,
        borderColor: "transparent",
        backgroundColor: "rgba(255, 59, 59, 0.1)",
        fill: "+1",
        pointRadius: 0,
        tension: 0.4,
      },
      {
        label: "Lower Bound",
        data: data.lower,
        borderColor: "transparent",
        backgroundColor: "rgba(255, 59, 59, 0.1)",
        fill: "-1",
        pointRadius: 0,
        tension: 0.4,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        mode: "index" as const,
        intersect: false,
        backgroundColor: "rgba(20, 25, 35, 0.9)",
        titleColor: "rgb(228, 230, 235)",
        bodyColor: "rgb(139, 146, 168)",
        borderColor: "rgb(42, 49, 66)",
        borderWidth: 1,
        padding: 8,
        displayColors: false,
        titleFont: {
          size: 11,
        },
        bodyFont: {
          size: 10,
        },
      },
    },
    scales: {
      x: {
        grid: {
          display: false,
          color: "rgb(42, 49, 66)",
        },
        ticks: {
          color: "rgb(139, 146, 168)",
          font: {
            size: 9,
          },
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: 8,
        },
      },
      y: {
        grid: {
          color: "rgb(42, 49, 66)",
          drawBorder: false,
        },
        ticks: {
          color: "rgb(139, 146, 168)",
          font: {
            size: 9,
          },
          callback: function(value: any) {
            return "$" + (value / 1000).toFixed(0) + "K";
          },
        },
      },
    },
  };

  return (
    <div className="h-full w-full relative">
      <Line data={chartData} options={options} />
      
      {/* Legend */}
      <div className="absolute top-0 right-0 flex items-center gap-3 text-[9px]">
        <div className="flex items-center gap-1">
          <div className="w-3 h-0.5 bg-primary" />
          <span className="text-muted-foreground">Baseline</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-3 h-0.5 bg-destructive border-dashed" />
          <span className="text-muted-foreground">Impact</span>
        </div>
      </div>
    </div>
  );
};

export default ForecastChart;