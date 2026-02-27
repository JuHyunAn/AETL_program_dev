import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
  MarkerType,
  Node,
  Edge,
  NodeMouseHandler,
  BackgroundVariant,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { Streamlit, withStreamlitConnection, ComponentProps } from "streamlit-component-lib";
import TableNode from "./TableNode";
import { getLayoutedElements } from "./layout";
import { ETLNodeDef, ETLEdgeDef, ETLNodeData } from "./types";

// ── 커스텀 노드 타입 등록 ─────────────────────────────────
const nodeTypes = { tableNode: TableNode };

// ── 레이어 배지 색상 (MiniMap용) ─────────────────────────
const MINIMAP_COLOR: Record<string, string> = {
  ods:    "#3B82F6",
  fact:   "#16A34A",
  dim:    "#0891B2",
  dm:     "#D97706",
  source: "#6B7280",
  custom: "#9333EA",
};

// ── 엣지 기본 스타일 ──────────────────────────────────────
const EDGE_STYLE = {
  strokeWidth: 2,
  stroke: "#94A3B8",
};

function buildFlowElements(
  rawNodes: ETLNodeDef[],
  rawEdges: ETLEdgeDef[],
  direction: "LR" | "TB"
): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = rawNodes.map((n) => ({
    id:       n.id,
    type:     "tableNode",
    position: { x: 0, y: 0 },     // dagre가 재배치
    data: {
      label:     n.label,
      layer:     n.layer,
      columns:   n.columns,
      col_count: n.col_count,
      db_type:   n.db_type,
    } as ETLNodeData,
  }));

  const edges: Edge[] = rawEdges.map((e) => ({
    id:             e.id,
    source:         e.source,
    target:         e.target,
    label:          e.label,
    animated:       true,
    markerEnd:      { type: MarkerType.ArrowClosed, width: 16, height: 16, color: "#64748B" },
    style:          EDGE_STYLE,
    labelStyle:     { fill: "#475569", fontWeight: 600, fontSize: 11 },
    labelBgStyle:   { fill: "#F8FAFC", opacity: 0.9 },
    labelBgPadding: [6, 4] as [number, number],
    labelBgBorderRadius: 4,
  }));

  return getLayoutedElements(nodes, edges, direction);
}

// ── 범례 컴포넌트 ─────────────────────────────────────────
function Legend() {
  const items = [
    { layer: "ods",    label: "ODS",    color: "#3B82F6" },
    { layer: "fact",   label: "FACT",   color: "#16A34A" },
    { layer: "dim",    label: "DIM",    color: "#0891B2" },
    { layer: "dm",     label: "DM",     color: "#D97706" },
    { layer: "custom", label: "TABLE",  color: "#9333EA" },
  ];
  return (
    <div style={{
      position: "absolute", bottom: 12, left: 12, zIndex: 10,
      display: "flex", gap: 8, flexWrap: "wrap",
      background: "rgba(255,255,255,0.92)",
      border: "1px solid #E2E8F0",
      borderRadius: 8, padding: "6px 10px",
      boxShadow: "0 1px 4px rgba(0,0,0,0.08)",
    }}>
      {items.map(({ layer, label, color }) => (
        <div key={layer} style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <div style={{ width: 10, height: 10, borderRadius: 2, background: color }} />
          <span style={{ fontSize: 11, color: "#475569", fontWeight: 600 }}>{label}</span>
        </div>
      ))}
      <span style={{ fontSize: 10, color: "#9CA3AF", marginLeft: 4, alignSelf: "center" }}>
        노드 클릭 → 컬럼 펼치기
      </span>
    </div>
  );
}

// ── 메인 컴포넌트 ─────────────────────────────────────────
function ETLFlowMap({ args }: ComponentProps) {
  const rawNodes:    ETLNodeDef[] = args.nodes    ?? [];
  const rawEdges:    ETLEdgeDef[] = args.edges    ?? [];
  const height:      number       = args.height   ?? 550;
  const direction:   "LR" | "TB" = args.direction ?? "LR";

  const { nodes: initNodes, edges: initEdges } = useMemo(
    () => buildFlowElements(rawNodes, rawEdges, direction),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [JSON.stringify(rawNodes), JSON.stringify(rawEdges), direction]
  );

  const [nodes, setNodes, onNodesChange] = useNodesState(initNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initEdges);

  // rawNodes/rawEdges 변경 시 재레이아웃
  useEffect(() => {
    const { nodes: ln, edges: le } = buildFlowElements(rawNodes, rawEdges, direction);
    setNodes(ln);
    setEdges(le);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(rawNodes), JSON.stringify(rawEdges), direction]);

  // 컴포넌트 높이를 Streamlit에 알림
  useEffect(() => {
    Streamlit.setFrameHeight(height);
  }, [height]);

  const onConnect = useCallback(
    (params: any) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  );

  // 노드 클릭 → Python으로 이벤트 전송
  const onNodeClick: NodeMouseHandler = useCallback((_evt, node) => {
    Streamlit.setComponentValue({ clicked_node: node.id });
  }, []);

  // 빈 데이터 안내
  if (rawNodes.length === 0) {
    return (
      <div style={{
        height, display: "flex", alignItems: "center", justifyContent: "center",
        background: "#F8FAFC", border: "1px dashed #CBD5E0", borderRadius: 10,
        flexDirection: "column", gap: 8, color: "#94A3B8",
        fontFamily: "'Pretendard','Noto Sans KR',sans-serif",
      }}>
        <div style={{ fontSize: 32 }}>◈</div>
        <div style={{ fontWeight: 600, fontSize: 14 }}>ETL Flow Map</div>
        <div style={{ fontSize: 12 }}>매핑 데이터를 등록하면 파이프라인 그래프가 표시됩니다</div>
      </div>
    );
  }

  return (
    <div style={{ width: "100%", height, position: "relative",
                  fontFamily: "'Pretendard','Noto Sans KR','Segoe UI',sans-serif" }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.3}
        maxZoom={2.0}
        defaultEdgeOptions={{ animated: true }}
        style={{ background: "#F8FAFC" }}
      >
        <Background variant={BackgroundVariant.Dots} gap={18} size={1} color="#E2E8F0" />
        <Controls
          showZoom
          showFitView
          showInteractive={false}
          style={{ boxShadow: "0 1px 4px rgba(0,0,0,0.1)" }}
        />
        <MiniMap
          nodeColor={(n) => {
            const layer = (n.data as unknown as ETLNodeData)?.layer ?? "custom";
            return MINIMAP_COLOR[layer] ?? "#9333EA";
          }}
          maskColor="rgba(248,250,252,0.7)"
          style={{ border: "1px solid #E2E8F0", borderRadius: 8 }}
        />
      </ReactFlow>

      <Legend />
    </div>
  );
}

export default withStreamlitConnection(ETLFlowMap);
