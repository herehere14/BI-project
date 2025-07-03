import React, { useState } from "react";
import { useNavigate } from "react-router-dom";

export default function BusinessType() {
  const nav = useNavigate();
  const [type, setType] = useState("");
  
  const industries = [
    { value: "e-commerce", label: "E-commerce & Retail", icon: "🛍️" },
    { value: "manufacturing", label: "Manufacturing", icon: "🏭" },
    { value: "finance", label: "Financial Services", icon: "💳" },
    { value: "healthcare", label: "Healthcare", icon: "🏥" },
    { value: "technology", label: "Technology & Software", icon: "💻" },
    { value: "logistics", label: "Logistics & Supply Chain", icon: "📦" },
    { value: "real-estate", label: "Real Estate", icon: "🏢" },
    { value: "hospitality", label: "Hospitality & Tourism", icon: "🏨" },
    { value: "education", label: "Education", icon: "🎓" },
    { value: "other", label: "Other", icon: "📊" }
  ];

  return (
    <>
      <div className="mb-8">
        <h2 className="text-3xl font-bold text-gray-900 mb-3">What's your industry?</h2>
        <p className="text-gray-600 text-lg">
          We'll customize Business Brain to provide insights specific to your sector
        </p>
      </div>

      <div className="space-y-3 mb-8">
        {industries.map((industry) => (
          <label
            key={industry.value}
            className={`
              flex items-center gap-4 p-4 border-2 rounded-xl cursor-pointer transition-all
              ${type === industry.value 
                ? 'border-blue-600 bg-blue-50' 
                : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
              }
            `}
          >
            <input
              type="radio"
              name="industry"
              value={industry.value}
              checked={type === industry.value}
              onChange={(e) => setType(e.target.value)}
              className="sr-only"
            />
            <span className="text-2xl">{industry.icon}</span>
            <div className="flex-1">
              <p className={`font-semibold ${type === industry.value ? 'text-blue-900' : 'text-gray-900'}`}>
                {industry.label}
              </p>
            </div>
            <div className={`
              w-5 h-5 rounded-full border-2 flex items-center justify-center
              ${type === industry.value 
                ? 'border-blue-600 bg-blue-600' 
                : 'border-gray-300'
              }
            `}>
              {type === industry.value && (
                <svg className="w-3 h-3 text-white" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                </svg>
              )}
            </div>
          </label>
        ))}
      </div>

      <button
        disabled={!type}
        onClick={() => nav("data-sources", { state: { biz_type: type } })}
        className={`
          w-full py-4 px-6 rounded-lg font-semibold text-white transition-all
          ${type 
            ? 'bg-blue-600 hover:bg-blue-700 shadow-lg hover:shadow-xl transform hover:-translate-y-0.5' 
            : 'bg-gray-300 cursor-not-allowed'
          }
        `}
      >
        Continue
        {type && (
          <svg className="inline-block ml-2 w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
          </svg>
        )}
      </button>
    </>
  );
}