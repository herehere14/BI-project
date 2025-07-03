import React, { useState } from "react";
import { useNavigate, useLocation } from "react-router-dom";

type SourceId =
  | "snowflake"
  | "bigquery"
  | "redshift"
  | "synapse"
  | "databricks"
  | "salesforce"
  | "google-analytics"
  | "stripe"
  | "hubspot"
  | "aws";

interface Source {
  id: SourceId;
  name: string;
  icon: string;
  needsDSN?: boolean;
}

export default function DataSources() {
  const nav = useNavigate();
  const { state } = useLocation();

  /** keep track of which sources the user enabled */
  const [selected, setSelected] = useState<Set<SourceId>>(new Set());

  /** DSNs keyed by source id */
  const [dsns, setDsns] = useState<Record<SourceId, string>>({} as any);

  /** files dragged-in by the user */
  const [files, setFiles] = useState<File[]>([]);

  const sources: Source[] = [
    { id: "snowflake", name: "Snowflake", icon: "❄️", needsDSN: true },
    { id: "bigquery", name: "BigQuery", icon: "🟦", needsDSN: true },
    { id: "redshift", name: "Redshift", icon: "🟥", needsDSN: true },
    { id: "synapse", name: "Azure Synapse", icon: "🔷", needsDSN: true },
    { id: "databricks", name: "Databricks", icon: "🟧", needsDSN: true },
    { id: "salesforce", name: "Salesforce", icon: "☁️" },
    { id: "google-analytics", name: "Google Analytics", icon: "📊" },
    { id: "stripe", name: "Stripe", icon: "💳" },
    { id: "hubspot", name: "HubSpot", icon: "🎯" },
    { id: "aws", name: "AWS", icon: "☁️" }
  ];

  /* ---------- helpers ---------- */
  const toggle = (id: SourceId) => {
    setSelected(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const handleDrop = (e: React.DragEvent<HTMLLabelElement>) => {
    e.preventDefault();
    const newFiles = Array.from(e.dataTransfer.files);
    setFiles(prev => [...prev, ...newFiles]);
  };

  /* ---------- render ---------- */
  return (
    <>
      <div className="mb-8">
        <h2 className="text-3xl font-bold mb-3">Connect your data sources</h2>
        <p className="text-gray-600 text-lg">
          Business Brain integrates with your warehouse &amp; SaaS tools to provide unified insights.
        </p>
      </div>

      {/* quick-select grid */}
      <div className="mb-8">
        <h3 className="text-sm font-semibold uppercase tracking-wider mb-4">
          Popular integrations
        </h3>

        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          {sources.map(s => (
            <button
              key={s.id}
              onClick={() => toggle(s.id)}
              className={`flex items-center gap-3 p-4 border-2 rounded-lg transition-all
                ${selected.has(s.id)
                  ? "border-blue-600 bg-blue-50"
                  : "border-gray-200 hover:border-gray-300 hover:bg-gray-50"}`}
            >
              <span className="text-2xl">{s.icon}</span>
              <span
                className={`font-medium ${
                  selected.has(s.id) ? "text-blue-900" : "text-gray-700"
                }`}
              >
                {s.name}
              </span>
              {selected.has(s.id) && (
                <svg
                  className="w-5 h-5 text-blue-600 ml-auto"
                  fill="currentColor"
                  viewBox="0 0 20 20"
                >
                  <path
                    fillRule="evenodd"
                    d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                    clipRule="evenodd"
                  />
                </svg>
              )}
            </button>
          ))}
        </div>
      </div>

      {/* DSN inputs for warehouses that need them */}
      {Array.from(selected)
        .filter(id => sources.find(s => s.id === id)?.needsDSN)
        .map(id => (
          <div className="mb-6" key={id}>
            <label
              htmlFor={`${id}-dsn`}
              className="block text-sm font-medium text-gray-700 mb-2"
            >
              {sources.find(s => s.id === id)?.name} connection string
            </label>
            <input
              id={`${id}-dsn`}
              type="text"
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
              placeholder="Account / DSN / JDBC URL"
              value={dsns[id] ?? ""}
              onChange={e => setDsns({ ...dsns, [id]: e.target.value })}
            />
          </div>
        ))}

      {/* drag-and-drop upload */}
      <div className="mb-10">
        <h3 className="text-sm font-semibold uppercase tracking-wider mb-4">
          Upload CSV / Excel
        </h3>
        <label
          onDragOver={e => e.preventDefault()}
          onDrop={handleDrop}
          className="flex flex-col items-center justify-center p-6 border-2 border-dashed border-gray-300 rounded-lg cursor-pointer hover:border-blue-400 hover:bg-blue-50 transition-colors"
        >
          <input
            type="file"
            accept=".csv, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel"
            multiple
            className="hidden"
            onChange={e => setFiles(prev => [...prev, ...Array.from(e.target.files ?? [])])}
          />
          <span className="text-4xl mb-2">📂</span>
          <span className="text-sm">
            Drag &amp; drop or click to upload (CSV / XLSX)
          </span>
        </label>

        {files.length > 0 && (
          <div className="mt-3 text-sm text-gray-700 space-y-1">
            {files.map(f => (
              <div key={f.name}>• {f.name}</div>
            ))}
          </div>
        )}
      </div>

      {/* info banner */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-8">
        <div className="flex gap-3">
          <svg
            className="w-5 h-5 text-blue-600 mt-0.5"
            fill="currentColor"
            viewBox="0 0 20 20"
          >
            <path
              fillRule="evenodd"
              d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z"
              clipRule="evenodd"
            />
          </svg>
          <div>
            <p className="font-semibold">Enterprise-grade security</p>
            <p className="text-sm text-blue-800">
              All connections are encrypted end-to-end. We never store raw credentials and are SOC 2 /
              GDPR compliant.
            </p>
          </div>
        </div>
      </div>

      {/* continue */}
      <button
        onClick={() =>
          nav("description", {
            state: {
              ...state,
              data_sources: Array.from(selected),
              dsns,
              uploaded_files: files
            }
          })
        }
        className="w-full py-4 px-6 bg-blue-600 text-white rounded-lg font-semibold hover:bg-blue-700 shadow-lg hover:shadow-xl transform hover:-translate-y-0.5 transition-all"
      >
        Continue
        <svg
          className="inline-block ml-2 w-5 h-5"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M13 7l5 5m0 0l-5 5m5-5H6"
          />
        </svg>
      </button>
    </>
  );
}
