import dagre from "@dagrejs/dagre";
import { Node, Edge } from "@xyflow/react";

const NODE_WIDTH = 260;
const NODE_HEIGHT_BASE = 80;

export function getLayoutedElements(
  nodes: Node[],
  edges: Edge[],
  direction: "LR" | "TB" = "TB"
): { nodes: Node[]; edges: Edge[] } {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));

  const isHorizontal = direction === "LR";
  g.setGraph({
    rankdir: direction,
    ranksep: isHorizontal ? 160 : 100,
    nodesep: isHorizontal ? 50 : 60,
    marginx: 30,
    marginy: 30,
  });

  for (const node of nodes) {
    const colCount = (node.data as any).col_count || 0;
    const h = NODE_HEIGHT_BASE + Math.min(colCount, 8) * 4;
    g.setNode(node.id, { width: NODE_WIDTH, height: h });
  }
  for (const edge of edges) {
    g.setEdge(edge.source, edge.target);
  }

  dagre.layout(g);

  const layoutedNodes = nodes.map((node) => {
    const pos = g.node(node.id);
    return {
      ...node,
      position: {
        x: pos.x - NODE_WIDTH / 2,
        y: pos.y - (pos.height || NODE_HEIGHT_BASE) / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
}
