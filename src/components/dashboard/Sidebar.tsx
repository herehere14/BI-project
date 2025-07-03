import React from "react";
import { Link, useLocation } from "react-router-dom";
import { motion } from "framer-motion";

interface SidebarProps {
  collapsed?: boolean;
}

const Sidebar: React.FC<SidebarProps> = ({ collapsed = false }) => {
  const location = useLocation();
  
  const menuItems = [
    { path: "/dashboard", label: "Dashboard", icon: "📊" },
    { path: "/forecasts", label: "Forecasts", icon: "📈" },
    { path: "/opportunities", label: "Opportunities", icon: "💡" },
    { path: "/automation", label: "Automation", icon: "⚡" },
    { path: "/reports", label: "Reports", icon: "📄" },
    { path: "/settings", label: "Settings", icon: "⚙️" },
  ];

  return (
    <aside className={`bg-card border-r border-border transition-all duration-300 ${
      collapsed ? "w-16" : "w-64"
    }`}>
      <nav className="p-4 space-y-2">
        {menuItems.map((item) => {
          const isActive = location.pathname === item.path;
          
          return (
            <Link
              key={item.path}
              to={item.path}
              className={`
                flex items-center gap-3 px-3 py-2 rounded-lg text-sm
                transition-all duration-200 relative
                ${isActive 
                  ? "bg-primary text-primary-foreground" 
                  : "text-muted-foreground hover:text-foreground hover:bg-popover"
                }
              `}
            >
              <span className="text-lg">{item.icon}</span>
              {!collapsed && (
                <motion.span
                  initial={{ opacity: 0, x: -10 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: -10 }}
                >
                  {item.label}
                </motion.span>
              )}
              
              {isActive && (
                <motion.div
                  layoutId="activeTab"
                  className="absolute left-0 top-0 bottom-0 w-1 bg-accent rounded-r"
                />
              )}
            </Link>
          );
        })}
      </nav>
    </aside>
  );
};

export default Sidebar;