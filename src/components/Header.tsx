import React from "react";

const Header: React.FC = () => (
  <header className="bg-medium border-b border-light">
    <div className="container mx-auto px-6 h-14 flex items-center justify-between">
      <h1 className="text-lg font-semibold tracking-wider text-primary">
        AI BIZ TERMINAL
      </h1>
      <div className="flex items-center gap-4">
        <span className="text-sm text-secondary hidden sm:block">v0.1.0</span>
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-positive"></div>
          <span className="text-xs text-secondary">Connected</span>
        </div>
      </div>
    </div>
  </header>
);

export default Header;