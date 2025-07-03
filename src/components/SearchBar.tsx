import React, { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { askAI } from "../hooks/api";

const SearchBar: React.FC = () => {
  const [query, setQuery] = useState("");

  const { mutate, isPending } = useMutation({
    mutationFn: askAI,
    onSuccess: (data) => {
      console.log("AI response:", data);
      setQuery("");
    },
    onError: (err) => console.error(err),
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) mutate({ query: query.trim() });
  };

  return (
    <form onSubmit={handleSubmit} className="bg-medium rounded-lg p-4 border border-light">
      <div className="flex gap-2">
        <input
          type="text"
          className="flex-1 bg-light/20 border border-light rounded px-3 py-2 text-sm text-primary placeholder:text-secondary focus:outline-none focus:ring-1 focus:ring-primary"
          placeholder="Ask AI about your business..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button
          type="submit"
          className="bg-primary text-white px-4 py-2 rounded text-sm font-medium hover:bg-primary/90 disabled:opacity-50"
          disabled={isPending || !query.trim()}
        >
          {isPending ? "Analyzing..." : "Ask"}
        </button>
      </div>
    </form>
  );
};

export default SearchBar;