export type LayerType = "ods" | "fact" | "dim" | "dm" | "source" | "custom";

export interface ColumnDef {
  name: string;
  type: string;
  pk?: boolean;
  nullable?: boolean;
}

export interface ETLNodeData extends Record<string, unknown> {
  label: string;
  layer: LayerType;
  columns: ColumnDef[];
  col_count: number;
  db_type?: string;
}

export interface ETLEdgeDef {
  id: string;
  source: string;
  target: string;
  label?: string;
}

export interface ETLNodeDef {
  id: string;
  label: string;
  layer: LayerType;
  columns: ColumnDef[];
  col_count: number;
  db_type?: string;
}

export interface FlowProps {
  nodes: ETLNodeDef[];
  edges: ETLEdgeDef[];
  height: number;
  direction: "LR" | "TB";
}
