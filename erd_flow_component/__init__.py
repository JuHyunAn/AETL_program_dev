"""
ERD Flow Component  —  DW 설계 ERD / 레이어 흐름도 시각화
React Flow 기반 Streamlit 커스텀 컴포넌트
"""
from __future__ import annotations

import os
from typing import Any

import streamlit.components.v1 as components

_COMPONENT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "build")

if os.path.isdir(_COMPONENT_DIR):
    _component_func = components.declare_component("erd_flow_map", path=_COMPONENT_DIR)
else:
    _component_func = components.declare_component(
        "erd_flow_map", url="http://localhost:3002"
    )


# ── 레이어 색상 매핑 (React 측과 동일) ──
_LAYER_COLORS = {
    "ods":  "#3B82F6",
    "fact": "#16A34A",
    "dim":  "#0891B2",
    "dm":   "#D97706",
}


def erd_flow_map(
    nodes: list[dict],
    edges: list[dict],
    height: int = 600,
    direction: str = "TB",
    mode: str = "erd",
    key: str | None = None,
) -> dict | None:
    """
    ERD / 레이어 흐름도를 React Flow로 렌더링합니다.

    Args:
        nodes: 테이블 노드 리스트
        edges: 관계 엣지 리스트
        height: 컴포넌트 높이 (px)
        direction: 레이아웃 방향 ("TB" | "LR")
        mode: "erd" (ERD 다이어그램) | "flow" (레이어 흐름도)
        key: Streamlit 위젯 키

    Returns:
        클릭된 노드 정보 또는 None
    """
    return _component_func(
        nodes=nodes,
        edges=edges,
        height=height,
        direction=direction,
        mode=mode,
        key=key,
        default=None,
    )


def build_erd_data(design: dict, layer: str = "all") -> tuple[list[dict], list[dict]]:
    """
    design_star_schema() 결과를 ERD 노드/엣지로 변환합니다.

    Args:
        design: design_star_schema()의 반환값
        layer: "all" | "ods" | "dw" | "dm"

    Returns:
        (nodes, edges) 튜플
    """
    nodes = []
    edges = []

    if layer in ("ods", "all"):
        for t in design.get("ods_tables", []):
            nodes.append(_make_node(t, "ods"))

    if layer in ("dw", "all"):
        for t in design.get("fact_tables", []):
            nodes.append(_make_node(t, "fact"))
        for t in design.get("dim_tables", []):
            nodes.append(_make_node(t, "dim"))

    if layer in ("dm", "all"):
        for t in design.get("dm_tables", []):
            nodes.append(_make_node(t, "dm"))

    node_ids = {n["id"] for n in nodes}
    for rel in design.get("relationships", []):
        src = rel.get("from", "")
        tgt = rel.get("to", "")
        if src in node_ids and tgt in node_ids:
            edges.append({
                "id": f"e_{src}_{tgt}",
                "source": src,
                "target": tgt,
                "label": rel.get("fk", ""),
                "relType": rel.get("type", "N:1"),
            })

    return nodes, edges


def build_flow_data(design: dict) -> tuple[list[dict], list[dict]]:
    """
    design_star_schema() 결과를 레이어 흐름도 노드/엣지로 변환합니다.

    Returns:
        (nodes, edges) 튜플
    """
    nodes = []
    edges = []

    for t in design.get("ods_tables", []):
        nodes.append(_make_node(t, "ods"))
    for t in design.get("fact_tables", []):
        nodes.append(_make_node(t, "fact"))
    for t in design.get("dim_tables", []):
        nodes.append(_make_node(t, "dim"))
    for t in design.get("dm_tables", []):
        nodes.append(_make_node(t, "dm"))

    # ODS → FACT 연결
    ods_names = [t["name"] for t in design.get("ods_tables", [])]
    fact_names = [t["name"] for t in design.get("fact_tables", [])]
    dim_names = [t["name"] for t in design.get("dim_tables", [])]
    dm_names = [t["name"] for t in design.get("dm_tables", [])]

    for ods in ods_names:
        for fact in fact_names:
            edges.append({
                "id": f"flow_{ods}_{fact}",
                "source": ods,
                "target": fact,
                "label": "ETL",
            })

    # FACT → DM 연결
    for fact in fact_names:
        for dm in dm_names:
            edges.append({
                "id": f"flow_{fact}_{dm}",
                "source": fact,
                "target": dm,
                "label": "AGG",
            })

    # DIM → FACT 연결 (역방향: DIM이 FACT에 조인)
    for rel in design.get("relationships", []):
        src = rel.get("from", "")
        tgt = rel.get("to", "")
        if src in fact_names and tgt in dim_names:
            edges.append({
                "id": f"flow_dim_{tgt}_{src}",
                "source": tgt,
                "target": src,
                "label": "JOIN",
            })

    return nodes, edges


def _make_node(tbl: dict, layer: str) -> dict:
    """테이블 딕셔너리를 노드 형식으로 변환합니다."""
    cols = tbl.get("columns", [])
    return {
        "id": tbl.get("name", "UNKNOWN"),
        "label": tbl.get("name", "UNKNOWN"),
        "layer": layer,
        "comment": tbl.get("comment", ""),
        "grain": tbl.get("grain", ""),
        "columns": [
            {
                "name": c.get("name", ""),
                "type": c.get("type", ""),
                "pk": bool(c.get("pk")),
                "nullable": c.get("nullable", True),
                "desc": c.get("desc", ""),
            }
            for c in cols
        ],
        "col_count": len(cols),
    }
