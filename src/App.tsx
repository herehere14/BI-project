// frontend/src/App.tsx
import React from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
  Outlet,
} from "react-router-dom";

import DashboardPage from "./pages/Dashboard";
import ForecastsPage from "./pages/Forecasts";
import OpportunitiesPage from "./pages/Opportunities";

import LoginPage from "./pages/Login";
import SignupPage from "./pages/Signup";

import OnboardingLayout from "./pages/onboarding/OnboardingLayout";
import BusinessType from "./pages/onboarding/BusinessType";
import DataSources from "./pages/onboarding/DataSources";
import Description from "./pages/onboarding/Description";

import { AuthProvider, AuthContext } from "./context/AuthProvider";

/* ------------------------------------------------------------------ */
/* Route guard: redirects to /login if no JWT token is present        */
const RequireAuth: React.FC = () => {
  const { token } = React.useContext(AuthContext);
  return token ? <Outlet /> : <Navigate to="/login" replace />;
};
/* ------------------------------------------------------------------ */

const App: React.FC = () => (
  <AuthProvider>
    <BrowserRouter>
      <Routes>
        {/* -------- Public routes -------- */}
        <Route path="/login" element={<LoginPage />} />
        <Route path="/signup" element={<SignupPage />} />

        {/* -------- On-boarding wizard -------- */}
        <Route path="/onboarding" element={<OnboardingLayout />}>
          <Route index element={<BusinessType />} />
          <Route path="data-sources" element={<DataSources />} />
          {/* --- FIX: description is nested under data-sources --- */}
          <Route path="data-sources/description" element={<Description />} />
        </Route>

        {/* -------- Protected app -------- */}
        <Route element={<RequireAuth />}>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/forecasts" element={<ForecastsPage />} />
          <Route path="/opportunities" element={<OpportunitiesPage />} />
        </Route>

        {/* -------- 404 fallback -------- */}
        <Route path="*" element={<p className="p-8">404 – Not found</p>} />
      </Routes>
    </BrowserRouter>
  </AuthProvider>
);

export default App;
