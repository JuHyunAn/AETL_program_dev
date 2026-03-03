"""
AETL ETL Flow Map — Streamlit Custom Component
React Flow 기반 인터랙티브 ETL 파이프라인 시각화
"""

import os
import subprocess
import sys
import streamlit.components.v1 as components

_COMPONENT_NAME = "etl_flow_map"
_FRONTEND_DIR   = os.path.join(os.path.dirname(__file__), "frontend", "build")
_SRC_DIR        = os.path.join(os.path.dirname(__file__), "frontend", "src")
_DEV_URL        = "http://localhost:3001"


def _auto_build() -> None:
    """
    build/ 폴더가 없거나 src/ 소스 파일이 빌드보다 새로울 때 자동으로 빌드합니다.
    - 최초 실행: npm install + npm run build 수행 (1~2분 소요)
    - 이후 실행: build/ 가 최신이면 즉시 스킵
    - src/ 수정 시: 다음 Streamlit 재시작 때 자동 재빌드
    """
    build_index = os.path.join(_FRONTEND_DIR, "index.html")

    # 소스 파일 최신 수정 시간
    src_mtime = 0.0
    if os.path.isdir(_SRC_DIR):
        for fname in os.listdir(_SRC_DIR):
            fpath = os.path.join(_SRC_DIR, fname)
            if os.path.isfile(fpath):
                src_mtime = max(src_mtime, os.path.getmtime(fpath))

    build_mtime = os.path.getmtime(build_index) if os.path.isfile(build_index) else 0.0

    if build_mtime >= src_mtime and os.path.isdir(_FRONTEND_DIR):
        return  # 빌드가 최신 → 스킵

    frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
    npm = "npm.cmd" if sys.platform == "win32" else "npm"

    print("[ETL Flow Map] React 컴포넌트 빌드를 시작합니다...")

    node_modules = os.path.join(frontend_dir, "node_modules")
    if not os.path.isdir(node_modules):
        print("[ETL Flow Map] npm install 실행 중...")
        subprocess.run([npm, "install"], cwd=frontend_dir, check=True)

    print("[ETL Flow Map] npm run build 실행 중...")
    subprocess.run([npm, "run", "build"], cwd=frontend_dir, check=True)
    print("[ETL Flow Map] 빌드 완료.")


# Streamlit 앱 시작 시 자동 빌드 시도
try:
    _auto_build()
except Exception as _build_err:
    print(f"[ETL Flow Map] 자동 빌드 실패 (개발 서버로 fallback): {_build_err}")

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

    # 스키마 접두사 → 레이어 매핑
    _SCHEMA_LAYER: dict[str, str] = {
        "ODS": "ods", "STG": "ods", "STAGING": "ods", "RAW": "ods",
        "SOURCE": "ods", "SRC": "ods",
        "DM": "dm", "MART": "dm", "MARTS": "dm",
        "ANALYTICS": "dm", "REPORT": "dm", "BI": "dm",
    }

    def _infer_layer(table_name: str) -> str:
        name = table_name.upper()

        # 스키마 포함 이름 처리 (예: "dw.dim_employee", "src.employee")
        if "." in name:
            schema, _, table = name.partition(".")
            if schema in _SCHEMA_LAYER:
                return _SCHEMA_LAYER[schema]
            # DW 스키마는 테이블명으로 세분화
            name = table

        if name.startswith(("ODS_", "STG_")):
            return "ods"
        if name.startswith(("FACT_", "DW_FACT")):
            return "fact"
        if name.startswith(("DIM_", "DW_DIM")):
            return "dim"
        if name.startswith(("DM_", "MART_")):
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
