"""
AETL ETL Flow Map — Streamlit Custom Component
React Flow 기반 인터랙티브 ETL 파이프라인 시각화
"""

import os
import streamlit.components.v1 as components

_COMPONENT_NAME = "etl_flow_map"
_FRONTEND_DIR   = os.path.join(os.path.dirname(__file__), "frontend", "build")
_DEV_URL        = "http://localhost:3001"

# 빌드된 파일이 있으면 production 모드, 없으면 개발 서버
if os.path.isdir(_FRONTEND_DIR):
    _component_func = components.declare_component(_COMPONENT_NAME, path=_FRONTEND_DIR)
else:
    _component_func = components.declare_component(_COMPONENT_NAME, url=_DEV_URL)


def etl_flow_map(
    nodes: list[dict],
    edges: list[dict],
    height: int = 550,
    direction: str = "LR",
    key: str | None = None,
) -> dict | None:
    """
    ETL 파이프라인을 인터랙티브 노드 그래프로 시각화합니다.

    Args:
        nodes: [
          {
            "id":       str,           # 고유 ID (테이블명 등)
            "label":    str,           # 노드 표시명
            "layer":    str,           # "ods" | "fact" | "dim" | "dm" | "source" | "custom"
            "columns":  list[dict],    # [{"name":str, "type":str, "pk":bool, "nullable":bool}]
            "col_count": int,
            "db_type":  str,           # optional
          }
        ]
        edges: [
          {
            "id":       str,
            "source":   str,           # source node id
            "target":   str,           # target node id
            "label":    str,           # "MERGE" | "INSERT" | "VIEW" 등
          }
        ]
        height:    컴포넌트 높이 (px)
        direction: 그래프 방향 "LR" (좌→우) | "TB" (위→아래)
        key:       Streamlit 위젯 키

    Returns:
        클릭된 노드 정보 {"clicked_node": id} 또는 None
    """
    return _component_func(
        nodes=nodes,
        edges=edges,
        height=height,
        direction=direction,
        key=key,
        default=None,
    )


def build_flow_data_from_mappings(mappings: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    AETL 매핑 결과 목록에서 Flow Map용 nodes/edges를 생성합니다.

    Args:
        mappings: [
          {
            "mapping_id":  str,
            "source_meta": {"table_name": str, "columns": [...], ...},
            "target_meta": {"table_name": str, "columns": [...], ...},
            "load_type":   str,
          }
        ]
    """
    node_map: dict[str, dict] = {}
    edges: list[dict] = []

    def _infer_layer(table_name: str) -> str:
        name = table_name.upper()
        if name.startswith("ODS_") or name.startswith("STG_"):
            return "ods"
        if name.startswith("FACT_") or name.startswith("DW_FACT"):
            return "fact"
        if name.startswith("DIM_") or name.startswith("DW_DIM"):
            return "dim"
        if name.startswith("DM_") or name.startswith("MART_"):
            return "dm"
        return "custom"

    for m in mappings:
        src = m.get("source_meta", {})
        tgt = m.get("target_meta", {})

        for meta in (src, tgt):
            tid = meta.get("table_name", "")
            if not tid or tid in node_map:
                continue
            cols = meta.get("columns", [])
            node_map[tid] = {
                "id":       tid,
                "label":    tid,
                "layer":    _infer_layer(tid),
                "columns":  [
                    {
                        "name":     c.get("name", c.get("column_name", "")),
                        "type":     c.get("type", c.get("data_type", "")),
                        "pk":       bool(c.get("pk", c.get("is_pk", False))),
                        "nullable": bool(c.get("nullable", True)),
                    }
                    for c in cols
                ],
                "col_count": len(cols),
            }

        src_id = src.get("table_name", "")
        tgt_id = tgt.get("table_name", "")
        if src_id and tgt_id:
            edges.append({
                "id":     f"e_{src_id}_{tgt_id}",
                "source": src_id,
                "target": tgt_id,
                "label":  m.get("load_type", "MERGE"),
            })

    return list(node_map.values()), edges
