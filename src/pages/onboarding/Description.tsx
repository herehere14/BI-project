import React, { useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

export default function Description() {
  const { state } = useLocation();
  const nav = useNavigate();
  const [desc, setDesc] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const prompts = [
    "What are your main products or services?",
    "Who are your primary customers?",
    "What are your key business objectives?",
    "What challenges are you looking to solve?"
  ];

  async function finish() {
    setIsSubmitting(true);
    try {
      await fetch("/company", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": "Bearer " + localStorage.getItem("token")
        },
        body: JSON.stringify({
          biz_type: state.biz_type,
          snowflake_dsn: state.snowflake_dsn,
          data_sources: state.data_sources,
          description: desc
        })
      });
      nav("/"); // dashboard
    } catch (error) {
      console.error("Error:", error);
      setIsSubmitting(false);
    }
  }

  return (
    <>
      <div className="mb-8">
        <h2 className="text-3xl font-bold text-gray-900 mb-3">Tell us about your business</h2>
        <p className="text-gray-600 text-lg">
          Help us understand your unique needs so we can deliver personalized insights
        </p>
      </div>

      <div className="space-y-6 mb-8">
        <div>
          <label htmlFor="description" className="block text-sm font-medium text-gray-700 mb-3">
            Business Overview
          </label>
          <textarea
            id="description"
            className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors resize-none"
            placeholder="Describe your business, goals, and what you hope to achieve with Business Brain..."
            rows={8}
            value={desc}
            onChange={(e) => setDesc(e.target.value)}
          />
          <div className="mt-2 flex justify-between text-sm">
            <span className="text-gray-500">
              Be as specific as possible for better customization
            </span>
            <span className={`${desc.length > 500 ? 'text-amber-600' : 'text-gray-500'}`}>
              {desc.length} / 500 recommended
            </span>
          </div>
        </div>

        {/* Helper Prompts */}
        <div className="bg-gray-50 rounded-lg p-4">
          <p className="text-sm font-medium text-gray-700 mb-3">Consider including:</p>
          <ul className="space-y-2">
            {prompts.map((prompt, index) => (
              <li key={index} className="flex items-start gap-2 text-sm text-gray-600">
                <svg className="w-4 h-4 text-blue-600 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                </svg>
                <span>{prompt}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>

      {/* Summary */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-8">
        <h4 className="font-semibold text-blue-900 mb-2">Setup Summary</h4>
        <div className="space-y-1 text-sm text-blue-800">
          <p>• Industry: <span className="font-medium">{state.biz_type?.replace('-', ' ').replace(/\b\w/g, l => l.toUpperCase())}</span></p>
          {state.data_sources?.length > 0 && (
            <p>• Connected Sources: <span className="font-medium">{state.data_sources.length} integrations</span></p>
          )}
          {state.snowflake_dsn && (
            <p>• Snowflake: <span className="font-medium">Configured</span></p>
          )}
        </div>
      </div>

      <button
        disabled={!desc || isSubmitting}
        onClick={finish}
        className={`
          w-full py-4 px-6 rounded-lg font-semibold text-white transition-all flex items-center justify-center
          ${desc && !isSubmitting
            ? 'bg-blue-600 hover:bg-blue-700 shadow-lg hover:shadow-xl transform hover:-translate-y-0.5' 
            : 'bg-gray-300 cursor-not-allowed'
          }
        `}
      >
        {isSubmitting ? (
          <>
            <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
            Setting up your dashboard...
          </>
        ) : (
          <>
            Complete Setup & Go to Dashboard
            <svg className="ml-2 w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
            </svg>
          </>
        )}
      </button>
    </>
  );
}