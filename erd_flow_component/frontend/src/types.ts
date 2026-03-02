export type LayerType = "ods" | "fact" | "dim" | "dm";

export interface ColumnDef {
  name: string;
  type: string;
  pk?: boolean;
  nullable?: boolean;
  desc?: string;
}

export interface ERDNodeData extends Record<string, unknown> {
  label: string;
  layer: LayerType;
  comment: string;
  grain: string;
  columns: ColumnDef[];
  col_count: number;
}

export interface ERDEdgeDef {
  id: string;
  source: string;
  target: string;
  label?: string;
  relType?: string;
}

export interface ERDNodeDef {
  id: string;
  label: string;
  layer: LayerType;
  comment: string;
  grain: string;
  columns: ColumnDef[];
  col_count: number;
}

export interface FlowProps {
  nodes: ERDNodeDef[];
  edges: ERDEdgeDef[];
  height: number;
  direction: "LR" | "TB";
  mode: "erd" | "flow";
}
