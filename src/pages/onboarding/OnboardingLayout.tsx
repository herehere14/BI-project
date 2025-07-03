import React from "react";
import { Outlet, useNavigate } from "react-router-dom";

export default function OnboardingLayout() {
  const nav = useNavigate();
  
  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-blue-50 flex flex-col">
      {/* Header */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-4xl mx-auto px-6 py-4">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-blue-600 rounded-lg flex items-center justify-center">
              <div className="w-5 h-5 bg-white rounded" />
            </div>
            <span className="text-xl font-bold text-gray-900">Business Brain</span>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex items-center justify-center p-6">
        <div className="w-full max-w-2xl">
          {/* Progress Bar */}
          <div className="mb-8">
            <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
              <div className="h-full bg-blue-600 rounded-full transition-all duration-500" 
                   style={{ width: '33%' }} />
            </div>
          </div>

          {/* Form Panel */}
          <div className="bg-white rounded-2xl shadow-xl border border-gray-100 p-10">
            <Outlet />
            
            {/* Navigation */}
            <div className="flex justify-between mt-8 pt-8 border-t border-gray-100">
              <button 
                onClick={() => nav(-1)} 
                className="flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors px-4 py-2 rounded-lg hover:bg-gray-50"
              >
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                </svg>
                Back
              </button>
              
              <button 
                onClick={() => nav(+1)} 
                className="flex items-center gap-2 text-gray-500 hover:text-gray-700 transition-colors px-4 py-2"
              >
                Skip for now
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
              </button>
            </div>
          </div>

          {/* Help Text */}
          <div className="mt-6 text-center text-sm text-gray-500">
            Need help? <a href="#" className="text-blue-600 hover:text-blue-700 font-medium">Contact support</a>
          </div>
        </div>
      </div>
    </div>
  );
}