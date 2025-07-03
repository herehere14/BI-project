import React from "react";

interface MiniChartProps {
  data: number[];
  color?: string;
  showArea?: boolean;
}

const MiniChart: React.FC<MiniChartProps> = ({ 
  data, 
  color = "rgb(0 163 255)", 
  showArea = false 
}) => {
  if (!data || data.length === 0) return null;

  const width = 100;
  const height = 20;
  const padding = 2;
  
  // Normalize data
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  
  // Create points
  const points = data.map((value, index) => {
    const x = padding + (index / (data.length - 1)) * (width - 2 * padding);
    const y = padding + ((max - value) / range) * (height - 2 * padding);
    return `${x},${y}`;
  }).join(" ");
  
  const areaPoints = `${padding},${height} ${points} ${width - padding},${height}`;

  return (
    <svg 
      viewBox={`0 0 ${width} ${height}`} 
      className="w-full h-full"
      preserveAspectRatio="none"
    >
      {showArea && (
        <polygon
          points={areaPoints}
          fill={color}
          opacity="0.1"
        />
      )}
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth="1.5"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
};

export default MiniChart;