import React, { useCallback, useEffect, useMemo } from "react";
import {
  ReactFlow,
  useNodesState,
  useEdgesState,
  Controls,
  MiniMap,
  Background,
  BackgroundVariant,
  MarkerType,
  Node,
  Edge,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { Streamlit, withStreamlitConnection, ComponentProps } from "streamlit-component-lib";

import ERDNode from "./ERDNode";
import { getLayoutedElements } from "./layout";
import type { ERDNodeDef, ERDEdgeDef, LayerType } from "./types";

const nodeTypes = { erdNode: ERDNode };

const LAYER_COLORS: Record<string, string> = {
  ods:  "#3B82F6",
  fact: "#16A34A",
  dim:  "#0891B2",
  dm:   "#D97706",
};

function buildFlowElements(
  rawNodes: ERDNodeDef[],
  rawEdges: ERDEdgeDef[],
  direction: "LR" | "TB"
) {
  const nodes: Node[] = rawNodes.map((n) => ({
    id: n.id,
    type: "erdNode",
    position: { x: 0, y: 0 },
    data: {
      label: n.label,
      layer: n.layer,
      comment: n.comment || "",
      grain: n.grain || "",
      columns: n.columns || [],
      col_count: n.col_count || 0,
    },
  }));

  const edges: Edge[] = rawEdges.map((e) => {
    const relLabel = e.relType
      ? `${e.label || ""} (${e.relType})`.trim()
      : e.label || "";

    return {
      id: e.id,
      source: e.source,
      target: e.target,
      label: relLabel,
      animated: false,
      style: { stroke: "#94A3B8", strokeWidth: 2 },
      markerEnd: { type: MarkerType.ArrowClosed, color: "#94A3B8" },
      labelStyle: { fontSize: 10, fill: "#64748B", fontWeight: 500 },
      labelBgStyle: { fill: "#F8FAFC", fillOpacity: 0.9 },
      labelBgPadding: [4, 2] as [number, number],
    };
  });

  return getLayoutedElements(nodes, edges, direction);
}

function Legend() {
  const items = [
    { label: "ODS", color: LAYER_COLORS.ods },
    { label: "FACT", color: LAYER_COLORS.fact },
    { label: "DIM", color: LAYER_COLORS.dim },
    { label: "DM", color: LAYER_COLORS.dm },
  ];

  return (
    <div
      style={{
        position: "absolute",
        bottom: 12,
        left: 12,
        background: "#ffffffee",
        border: "1px solid #E2E8F0",
        borderRadius: 8,
        padding: "8px 12px",
        fontSize: 11,
        zIndex: 10,
        display: "flex",
        gap: 12,
        alignItems: "center",
      }}
    >
      {items.map((it) => (
        <div key={it.label} style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <div
            style={{
              width: 10,
              height: 10,
              borderRadius: 2,
              background: it.color,
            }}
          />
          <span>{it.label}</span>
        </div>
      ))}
      <span style={{ color: "#94A3B8", marginLeft: 8 }}>
        클릭 → 컬럼 펼치기
      </span>
    </div>
  );
}

function ERDFlowMap(props: ComponentProps) {
  const rawNodes: ERDNodeDef[] = props.args?.nodes || [];
  const rawEdges: ERDEdgeDef[] = props.args?.edges || [];
  const height: number = props.args?.height || 600;
  const direction: "LR" | "TB" = props.args?.direction || "TB";
  const mode: string = props.args?.mode || "erd";

  const { nodes: layoutedNodes, edges: layoutedEdges } = useMemo(
    () => buildFlowElements(rawNodes, rawEdges, direction),
    [rawNodes, rawEdges, direction]
  );

  const [nodes, setNodes, onNodesChange] = useNodesState(layoutedNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(layoutedEdges);

  useEffect(() => {
    setNodes(layoutedNodes);
    setEdges(layoutedEdges);
  }, [layoutedNodes, layoutedEdges, setNodes, setEdges]);

  useEffect(() => {
    Streamlit.setFrameHeight(height);
  }, [height]);

  const onNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    Streamlit.setComponentValue({ clicked_node: node.id });
  }, []);

  if (rawNodes.length === 0) {
    return (
      <div
        style={{
          height,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "#94A3B8",
          fontSize: 14,
          background: "#F8FAFC",
          borderRadius: 8,
          border: "1px dashed #CBD5E1",
        }}
      >
        {mode === "erd" ? "ERD 다이어그램" : "레이어 흐름도"}: 설계 데이터가 없습니다.
      </div>
    );
  }

  return (
    <div style={{ width: "100%", height, position: "relative" }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.2}
        maxZoom={2.5}
      >
        <Background variant={BackgroundVariant.Dots} color="#CBD5E1" gap={20} size={1} />
        <Controls />
        <MiniMap
          nodeColor={(node: Node) => {
            const layer = (node.data as any)?.layer as string;
            return LAYER_COLORS[layer] || "#6B7280";
          }}
          style={{ bottom: 48, right: 12 }}
          zoomable
          pannable
        />
      </ReactFlow>
      <Legend />
    </div>
  );
}

export default withStreamlitConnection(ERDFlowMap);
