import React from "react";
import Header from "./Header";

interface Props {
  children: React.ReactNode;
}

/**
 * Root layout used across all pages.
 *
 * • Uses the `dashboard-grid` utility (defined in globals.css)
 *   → 300 px | 1fr | 400 px
 * • `w-full` forces the grid to stretch the full viewport width,
 *   preventing the compressed centre column.
 */
const Layouts: React.FC<Props> = ({ children }) => (
  <div className="min-h-screen w-screen flex flex-col bg-background text-foreground">
    {/* Top bar */}
    <Header />

    {/* Main dashboard grid */}
    <main className="dashboard-grid w-full flex-1 overflow-hidden">
      {children}
    </main>
  </div>
);

export default Layouts;
