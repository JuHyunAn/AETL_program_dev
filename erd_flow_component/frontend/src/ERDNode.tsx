import React, { useState, memo } from "react";
import { Handle, Position } from "@xyflow/react";
import type { ERDNodeData, ColumnDef, LayerType } from "./types";

const LAYER_CONFIG: Record<LayerType, { color: string; bg: string; badge: string }> = {
  ods:  { color: "#3B82F6", bg: "#EFF6FF", badge: "ODS" },
  fact: { color: "#16A34A", bg: "#F0FDF4", badge: "FACT" },
  dim:  { color: "#0891B2", bg: "#ECFEFF", badge: "DIM" },
  dm:   { color: "#D97706", bg: "#FFFBEB", badge: "DM" },
};

function ERDNode({ data }: { data: ERDNodeData }) {
  const [expanded, setExpanded] = useState(false);
  const layer = data.layer as LayerType;
  const cfg = LAYER_CONFIG[layer] || LAYER_CONFIG.ods;
  const columns: ColumnDef[] = data.columns || [];

  return (
    <div
      style={{
        background: "#fff",
        border: `2px solid ${cfg.color}`,
        borderRadius: 8,
        minWidth: 240,
        maxWidth: 280,
        fontSize: 12,
        fontFamily: "'Pretendard', 'Segoe UI', sans-serif",
        boxShadow: "0 2px 8px rgba(0,0,0,0.08)",
        overflow: "hidden",
      }}
    >
      <Handle type="target" position={Position.Top} style={{ background: cfg.color }} />
      <Handle type="source" position={Position.Bottom} style={{ background: cfg.color }} />

      {/* Header */}
      <div
        onClick={() => setExpanded(!expanded)}
        style={{
          background: cfg.bg,
          borderBottom: `1px solid ${cfg.color}30`,
          padding: "8px 10px",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          gap: 6,
        }}
      >
        <span
          style={{
            background: cfg.color,
            color: "#fff",
            fontSize: 10,
            fontWeight: 700,
            padding: "2px 6px",
            borderRadius: 4,
          }}
        >
          {cfg.badge}
        </span>
        <span style={{ fontWeight: 700, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {data.label}
        </span>
        <span style={{ color: "#94A3B8", fontSize: 10 }}>{data.col_count}열</span>
        <span style={{ color: "#94A3B8", fontSize: 10, transform: expanded ? "rotate(180deg)" : "none", transition: "transform 0.2s" }}>
          ▼
        </span>
      </div>

      {/* Comment / Grain */}
      {(data.comment || data.grain) && (
        <div style={{ padding: "4px 10px", color: "#64748B", fontSize: 11, borderBottom: `1px solid #F1F5F9` }}>
          {data.comment && <div>{data.comment}</div>}
          {data.grain && <div style={{ fontStyle: "italic", color: "#94A3B8" }}>Grain: {data.grain}</div>}
        </div>
      )}

      {/* Columns (expandable) */}
      {expanded && columns.length > 0 && (
        <div style={{ maxHeight: 240, overflowY: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ background: "#F8FAFC", borderBottom: "1px solid #E2E8F0" }}>
                <th style={{ padding: "4px 6px", textAlign: "left", fontWeight: 600, width: 20 }}></th>
                <th style={{ padding: "4px 6px", textAlign: "left", fontWeight: 600 }}>Column</th>
                <th style={{ padding: "4px 6px", textAlign: "left", fontWeight: 600 }}>Type</th>
              </tr>
            </thead>
            <tbody>
              {columns.map((col, i) => (
                <tr
                  key={i}
                  style={{
                    borderBottom: "1px solid #F1F5F9",
                    background: col.pk ? "#FFFBEB" : "transparent",
                  }}
                >
                  <td style={{ padding: "3px 6px", textAlign: "center" }}>
                    {col.pk && <span style={{ color: "#F59E0B", fontWeight: 700, fontSize: 10 }}>PK</span>}
                  </td>
                  <td
                    style={{ padding: "3px 6px", fontWeight: col.pk ? 600 : 400 }}
                    title={col.desc || ""}
                  >
                    {col.name}
                  </td>
                  <td style={{ padding: "3px 6px", color: "#64748B" }}>{col.type}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Collapsed summary */}
      {!expanded && columns.length > 0 && (
        <div style={{ padding: "4px 10px", color: "#94A3B8", fontSize: 10 }}>
          {columns.filter(c => c.pk).map(c => c.name).join(", ") || "PK 없음"}
          {" | "}
          {columns.length}개 컬럼
        </div>
      )}
    </div>
  );
}

export default memo(ERDNode);
