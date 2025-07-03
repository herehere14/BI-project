import React from "react";
import { motion } from "framer-motion";

interface EfficiencyGaugeProps {
  value: number;
  target: number;
  industry: number;
}

const EfficiencyGauge: React.FC<EfficiencyGaugeProps> = ({ 
  value = 78, 
  target = 85, 
  industry = 72 
}) => {
  // Use dynamic radius based on container size
  const [radius, setRadius] = React.useState(80);
  
  const calculateDimensions = () => {
    // Adjust size based on screen width
    if (typeof window === 'undefined') return;
    
    const newRadius = window.innerWidth < 640 ? 60 : 80;
    setRadius(newRadius);
  };

  React.useEffect(() => {
    calculateDimensions();
    window.addEventListener('resize', calculateDimensions);
    return () => window.removeEventListener('resize', calculateDimensions);
  }, []);

  const strokeWidth = radius * 0.15; // Responsive stroke width
  const normalizedRadius = radius - strokeWidth / 2;
  const circumference = normalizedRadius * 2 * Math.PI;

  const getStrokeDashoffset = (val: number) => {
    const offset = circumference - (val / 100) * circumference * 0.75;
    return offset;
  };

  const getColor = (val: number) => {
    if (val >= 90) return "rgb(0, 255, 136)";
    if (val >= 70) return "rgb(0, 163, 255)";
    if (val >= 50) return "rgb(255, 184, 0)";
    return "rgb(255, 59, 59)";
  };

  return (
    <div className="h-full flex flex-col items-center justify-center">
      <div className="relative w-full max-w-[200px] aspect-square">
        <svg
          height={radius * 2}
          width={radius * 2}
          viewBox={`0 0 ${radius * 2} ${radius * 2}`}
          className="transform -rotate-90"
        >
          {/* Background arc */}
          <circle
            stroke="rgb(42, 49, 66)"
            fill="transparent"
            strokeWidth={strokeWidth}
            strokeDasharray={`${circumference * 0.75} ${circumference}`}
            r={normalizedRadius}
            cx={radius}
            cy={radius}
          />
          
          {/* Industry benchmark */}
          <circle
            stroke="rgb(139, 146, 168)"
            fill="transparent"
            strokeWidth={strokeWidth * 0.4}
            strokeDasharray={`2 4`}
            strokeDashoffset={getStrokeDashoffset(industry)}
            r={normalizedRadius}
            cx={radius}
            cy={radius}
          />
          
          {/* Target line */}
          <circle
            stroke="rgb(255, 184, 0)"
            fill="transparent"
            strokeWidth={strokeWidth * 0.4}
            strokeDasharray={`4 ${circumference}`}
            strokeDashoffset={getStrokeDashoffset(target)}
            r={normalizedRadius}
            cx={radius}
            cy={radius}
          />
          
          {/* Value arc */}
          <motion.circle
            stroke={getColor(value)}
            fill="transparent"
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            strokeDasharray={`${circumference * 0.75} ${circumference}`}
            initial={{ strokeDashoffset: circumference * 0.75 }}
            animate={{ strokeDashoffset: getStrokeDashoffset(value) }}
            transition={{ duration: 1, ease: "easeOut" }}
            r={normalizedRadius}
            cx={radius}
            cy={radius}
          />
        </svg>
        
        {/* Center value */}
        <div className="absolute inset-0 flex items-center justify-center transform rotate-90">
          <motion.div
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            transition={{ delay: 0.5, type: "spring" }}
            className="text-center"
          >
            <div className="text-3xl font-bold text-foreground">{value}%</div>
            <div className="text-xs text-muted-foreground">Efficiency</div>
          </motion.div>
        </div>
      </div>
      
      {/* Legend */}
      <div className="mt-4 space-y-2 w-full max-w-[200px] px-2">
        <div className="flex items-center justify-between text-xs">
          <div className="flex items-center gap-2">
            <div 
              className="w-3 h-3 rounded-full" 
              style={{ backgroundColor: getColor(value) }} 
            />
            <span>Current</span>
          </div>
          <span className="font-mono font-semibold">{value}%</span>
        </div>
        
        <div className="flex items-center justify-between text-xs">
          <div className="flex items-center gap-2">
            <div className="w-3 h-0.5 bg-warning" />
            <span className="text-muted-foreground">Target</span>
          </div>
          <span className="font-mono text-muted-foreground">{target}%</span>
        </div>
        
        <div className="flex items-center justify-between text-xs">
          <div className="flex items-center gap-2">
            <div 
              className="w-3 h-0.5 border-t border-dashed border-muted-foreground" 
              style={{ borderTopWidth: "2px" }}
            />
            <span className="text-muted-foreground">Industry</span>
          </div>
          <span className="font-mono text-muted-foreground">{industry}%</span>
        </div>
      </div>
      
      {/* Status message */}
      <div className="mt-4 text-center">
        {value >= target ? (
          <p className="text-xs text-accent">▲ Above target by {value - target}%</p>
        ) : (
          <p className="text-xs text-warning">▼ Below target by {target - value}%</p>
        )}
      </div>
    </div>
  );
};

export default EfficiencyGauge;