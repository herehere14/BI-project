import React, { useState, DragEvent } from "react";

const UploadDropzone: React.FC = () => {
  const [hover, setHover] = useState(false);
  async function handleFile(f: File) {
    const body = new FormData(); body.append("file", f);
    const r = await fetch("/ingest/file", { method: "POST", body });
    if (!r.ok) alert(await r.text()); else alert("Uploaded!");
  }
  function onDrop(e: DragEvent) {
    e.preventDefault(); setHover(false);
    if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]);
  }
  return (
    <div
      onDragOver={(e)=>{e.preventDefault(); setHover(true);}}
      onDragLeave={()=>setHover(false)}
      onDrop={onDrop}
      className={`border-2 border-dashed rounded-lg p-8 text-center cursor-pointer
                  ${hover ? "border-primary bg-primary/5" : "border-border"}`}
      onClick={()=>document.getElementById("uploader")?.click()}
    >
      <p className="text-sm">Drag &amp; drop a CSV / Parquet / Excel file<br/>or click to choose</p>
      <input
        id="uploader" type="file" accept=".csv,.parquet,.pq,.xlsx,.xls"
        className="hidden" onChange={e=>e.target.files?.[0] && handleFile(e.target.files[0])}
      />
    </div>
  );
};

export default UploadDropzone;
