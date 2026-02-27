import React, { useState, memo } from "react";
import { Handle, Position, NodeProps } from "@xyflow/react";
import { ETLNodeData, ColumnDef } from "./types";

// ── 레이어별 스타일 설정 ──────────────────────────────────
const LAYER_CONFIG: Record<string, {
  bg: string; border: string; badge: string;
  badgeText: string; headerBg: string;
}> = {
  ods:    { bg: "#EFF6FF", border: "#3B82F6", badge: "#3B82F6",  badgeText: "ODS",    headerBg: "#DBEAFE" },
  fact:   { bg: "#F0FDF4", border: "#16A34A", badge: "#16A34A",  badgeText: "FACT",   headerBg: "#DCFCE7" },
  dim:    { bg: "#F0F9FF", border: "#0891B2", badge: "#0891B2",  badgeText: "DIM",    headerBg: "#CFFAFE" },
  dm:     { bg: "#FFFBEB", border: "#D97706", badge: "#D97706",  badgeText: "DM",     headerBg: "#FEF3C7" },
  source: { bg: "#F9FAFB", border: "#6B7280", badge: "#6B7280",  badgeText: "SOURCE", headerBg: "#F3F4F6" },
  custom: { bg: "#FDF4FF", border: "#9333EA", badge: "#9333EA",  badgeText: "TABLE",  headerBg: "#F3E8FF" },
};

const DEFAULT_CONFIG = LAYER_CONFIG.custom;

function TableNode({ data, selected }: NodeProps) {
  const nodeData = data as unknown as ETLNodeData;
  const [expanded, setExpanded] = useState(false);
  const cfg = LAYER_CONFIG[nodeData.layer] ?? DEFAULT_CONFIG;

  const containerStyle: React.CSSProperties = {
    background: cfg.bg,
    border: `2px solid ${selected ? "#1D4ED8" : cfg.border}`,
    borderRadius: 10,
    minWidth: 210,
    maxWidth: 240,
    boxShadow: selected
      ? "0 0 0 3px rgba(29,78,216,0.25)"
      : "0 2px 8px rgba(0,0,0,0.10)",
    fontFamily: "'Pretendard', 'Noto Sans KR', 'Segoe UI', sans-serif",
    fontSize: 12,
    cursor: "default",
    transition: "box-shadow 0.15s",
  };

  const headerStyle: React.CSSProperties = {
    background: cfg.headerBg,
    borderRadius: "8px 8px 0 0",
    padding: "9px 12px 8px",
    display: "flex",
    alignItems: "center",
    gap: 7,
    cursor: "pointer",
    userSelect: "none",
    borderBottom: `1px solid ${cfg.border}40`,
  };

  const badgeStyle: React.CSSProperties = {
    background: cfg.badge,
    color: "#fff",
    borderRadius: 4,
    padding: "1px 7px",
    fontSize: 10,
    fontWeight: 700,
    letterSpacing: "0.04em",
    flexShrink: 0,
  };

  const tableNameStyle: React.CSSProperties = {
    fontWeight: 700,
    fontSize: 12.5,
    color: "#111827",
    flex: 1,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  };

  const countStyle: React.CSSProperties = {
    fontSize: 10,
    color: "#6B7280",
    flexShrink: 0,
  };

  const chevronStyle: React.CSSProperties = {
    fontSize: 10,
    color: "#9CA3AF",
    flexShrink: 0,
    transition: "transform 0.2s",
    transform: expanded ? "rotate(180deg)" : "rotate(0deg)",
  };

  const columnsWrapStyle: React.CSSProperties = {
    maxHeight: 200,
    overflowY: "auto",
    borderTop: "none",
  };

  const colRowStyle = (isPk: boolean): React.CSSProperties => ({
    padding: "4px 12px",
    display: "flex",
    alignItems: "center",
    gap: 5,
    borderBottom: "1px solid #F1F5F9",
    background: isPk ? `${cfg.bg}` : "transparent",
  });

  const pkBadgeStyle: React.CSSProperties = {
    background: "#F59E0B",
    color: "#fff",
    borderRadius: 3,
    padding: "0px 5px",
    fontSize: 9,
    fontWeight: 700,
    flexShrink: 0,
  };

  const colNameStyle = (isPk: boolean): React.CSSProperties => ({
    flex: 1,
    fontWeight: isPk ? 600 : 400,
    color: "#1F2937",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  });

  const colTypeStyle: React.CSSProperties = {
    color: "#9CA3AF",
    fontSize: 10,
    flexShrink: 0,
    maxWidth: 80,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  };

  return (
    <div style={containerStyle}>
      {/* ← Source 핸들 (왼쪽) */}
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: cfg.border, width: 10, height: 10, border: "2px solid #fff" }}
      />

      {/* 헤더 (클릭으로 컬럼 토글) */}
      <div style={headerStyle} onClick={() => setExpanded((p) => !p)}>
        <span style={badgeStyle}>{cfg.badgeText}</span>
        <span style={tableNameStyle} title={nodeData.label}>{nodeData.label}</span>
        <span style={countStyle}>{nodeData.col_count}열</span>
        <span style={chevronStyle}>▼</span>
      </div>

      {/* 컬럼 목록 (펼치면 표시) */}
      {expanded && (
        <div style={columnsWrapStyle}>
          {nodeData.columns.length === 0 ? (
            <div style={{ padding: "8px 12px", color: "#9CA3AF", fontSize: 11 }}>
              컬럼 정보 없음
            </div>
          ) : (
            nodeData.columns.map((col: ColumnDef) => (
              <div key={col.name} style={colRowStyle(!!col.pk)}>
                {col.pk && <span style={pkBadgeStyle}>PK</span>}
                <span style={colNameStyle(!!col.pk)} title={col.name}>
                  {col.name}
                </span>
                <span style={colTypeStyle} title={col.type}>
                  {col.type}
                </span>
              </div>
            ))
          )}
        </div>
      )}

      {/* → Target 핸들 (오른쪽) */}
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: cfg.border, width: 10, height: 10, border: "2px solid #fff" }}
      />
    </div>
  );
}

export default memo(TableNode);
