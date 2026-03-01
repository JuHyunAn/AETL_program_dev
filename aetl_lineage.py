"""
================================================================================
AETL Lineage Engine  —  v2.1
================================================================================
역할 분리 원칙:
  - sqlglot (규칙 기반 파서): SQL → AST → 컬럼 리니지 추출
  - NetworkX: DAG 구성, Forward/Backward 탐색
  - LLM: 리니지 결과를 한국어로 설명 (파싱은 절대 안 맡김)
  - Mermaid: Streamlit 내 시각화
================================================================================
"""

from __future__ import annotations

import os
import re
from typing import Any

import networkx as nx
import sqlglot
from sqlglot import exp


# ─────────────────────────────────────────────────────────────
# 1. SQL 파싱 → 리니지 추출 (sqlglot)
# ─────────────────────────────────────────────────────────────

def parse_lineage(sql: str, db_type: str = "oracle") -> dict:
    """
    SQL에서 테이블 및 컬럼 리니지를 추출합니다.
    ★ sqlglot 규칙 기반 파서 사용 (LLM 없음, 100% 결정론적)

    Returns:
        {
          "source_tables":  [str],
          "target_table":   str | None,
          "column_lineage": [
            {"target_col": str, "source_col": str, "source_table": str, "transform": str}
          ],
          "table_lineage":  [{"from": str, "to": str}],
          "ctes":           [str],
          "error":          str | None,
        }
    """
    dialect = {"oracle": "oracle", "mariadb": "mysql", "postgresql": "postgres"}.get(db_type, "ansi")
    result = {
        "source_tables": [], "target_table": None,
        "column_lineage": [], "table_lineage": [],
        "ctes": [], "error": None,
    }

    try:
        statements = sqlglot.parse(sql.strip(), dialect=dialect)
    except Exception as e:
        result["error"] = f"파싱 실패: {e}"
        return result

    for stmt in statements:
        if stmt is None:
            continue

        # CTE 수집
        for cte in stmt.find_all(exp.CTE):
            alias = cte.alias
            if alias:
                result["ctes"].append(alias)

        # INSERT INTO target_table SELECT ...
        if isinstance(stmt, exp.Insert):
            target = stmt.find(exp.Table)
            if target:
                result["target_table"] = _table_name(target)

        # CREATE TABLE target AS SELECT ...
        if isinstance(stmt, exp.Create):
            target = stmt.find(exp.Table)
            if target:
                result["target_table"] = _table_name(target)

        # 소스 테이블 수집 (FROM / JOIN)
        for tbl in stmt.find_all(exp.Table):
            name = _table_name(tbl)
            if name and name not in result["ctes"]:
                if name != result["target_table"]:
                    if name not in result["source_tables"]:
                        result["source_tables"].append(name)

        # 컬럼 리니지 (SELECT 절)
        select = stmt.find(exp.Select)
        if select:
            for sel_expr in select.expressions:
                col_info = _extract_column_lineage(sel_expr)
                if col_info:
                    result["column_lineage"].append(col_info)

    # 테이블 리니지
    target = result["target_table"] or "OUTPUT"
    for src in result["source_tables"]:
        result["table_lineage"].append({"from": src, "to": target})

    return result


def _table_name(tbl: exp.Table) -> str | None:
    parts = []
    if tbl.db:
        parts.append(str(tbl.db))
    if tbl.name:
        parts.append(str(tbl.name))
    return ".".join(parts) if parts else None


def _extract_column_lineage(sel_expr) -> dict | None:
    """SELECT 표현식 하나에서 컬럼 리니지 정보 추출"""
    try:
        # 대상 컬럼명 (alias or 원본명)
        if isinstance(sel_expr, exp.Alias):
            target_col  = str(sel_expr.alias)
            inner       = sel_expr.this
        else:
            target_col  = str(sel_expr)
            inner       = sel_expr

        # 소스 컬럼
        source_col   = ""
        source_table = ""
        transform    = ""

        if isinstance(inner, exp.Column):
            source_col   = str(inner.name)
            source_table = str(inner.table) if inner.table else ""
        elif isinstance(inner, (exp.Anonymous, exp.Func)):
            # 함수 변환 (COALESCE, TO_DATE, SUBSTR 등)
            transform = str(inner)[:80]
            # 함수 내 첫 번째 컬럼을 소스로
            cols_in_func = list(inner.find_all(exp.Column))
            if cols_in_func:
                source_col   = str(cols_in_func[0].name)
                source_table = str(cols_in_func[0].table) if cols_in_func[0].table else ""
        elif isinstance(inner, exp.Literal):
            source_col = str(inner)
            transform  = "LITERAL"
        else:
            transform = str(inner)[:80]

        return {
            "target_col":   target_col,
            "source_col":   source_col,
            "source_table": source_table,
            "transform":    transform,
        }
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# 2. NetworkX DAG 구성
# ─────────────────────────────────────────────────────────────

def build_lineage_graph(lineage: dict) -> nx.DiGraph:
    """
    리니지 정보로 방향 그래프(DAG)를 생성합니다.

    노드: "TABLE.COLUMN" 형식
    엣지: source → target (컬럼 레벨)
    """
    G = nx.DiGraph()

    target_tbl = lineage.get("target_table") or "OUTPUT"

    for col_info in lineage.get("column_lineage", []):
        src_tbl = col_info.get("source_table", "").upper() or (
            lineage["source_tables"][0].upper() if lineage["source_tables"] else "SOURCE"
        )
        src_col  = col_info.get("source_col",  "").upper()
        tgt_col  = col_info.get("target_col",  "").upper()
        transform = col_info.get("transform", "")

        src_node = f"{src_tbl}.{src_col}" if src_col else src_tbl
        tgt_node = f"{target_tbl}.{tgt_col}"

        G.add_node(src_node, table=src_tbl, column=src_col, layer="source")
        G.add_node(tgt_node, table=target_tbl, column=tgt_col, layer="target")
        G.add_edge(src_node, tgt_node, transform=transform)

    return G


def get_impact(G: nx.DiGraph, node: str, direction: str = "forward") -> list[str]:
    """
    특정 노드의 영향도를 탐색합니다.

    Args:
        direction: "forward"  → 이 컬럼이 영향을 주는 하위 컬럼들
                   "backward" → 이 컬럼에 영향을 주는 상위 컬럼들
    """
    if node not in G:
        return []
    if direction == "forward":
        return list(nx.descendants(G, node))
    else:
        return list(nx.ancestors(G, node))


# ─────────────────────────────────────────────────────────────
# 3. Mermaid 시각화
# ─────────────────────────────────────────────────────────────

def generate_mermaid_lineage(lineage: dict, max_cols: int = 20) -> str:
    """
    리니지 결과를 Mermaid flowchart LR로 변환합니다.

    Args:
        max_cols: 표시할 최대 컬럼 수 (가독성 제한)
    """
    lines = ["flowchart LR"]

    source_tables = lineage.get("source_tables", [])
    target_table  = lineage.get("target_table") or "OUTPUT"
    col_lineage   = lineage.get("column_lineage", [])[:max_cols]
    ctes          = lineage.get("ctes", [])

    # 소스 테이블 서브그래프
    src_col_map: dict[str, list[str]] = {}
    for c in col_lineage:
        tbl = c.get("source_table", "").upper() or (source_tables[0].upper() if source_tables else "SRC")
        src_col_map.setdefault(tbl, []).append(c.get("source_col", ""))

    for tbl in source_tables:
        safe_id = _safe_id(tbl)
        lines.append(f"    subgraph {safe_id}[{tbl}]")
        for col in src_col_map.get(tbl.upper(), [])[:10]:
            col_id = _safe_id(f"{tbl}_{col}")
            lines.append(f'        {col_id}["{col}"]')
        lines.append("    end")

    # CTE 서브그래프
    for cte in ctes[:3]:
        safe_id = _safe_id(cte)
        lines.append(f"    subgraph {safe_id}[CTE: {cte}]")
        lines.append(f'        {safe_id}_data[("집계/변환")]')
        lines.append("    end")

    # 타겟 서브그래프
    tgt_id = _safe_id(target_table)
    lines.append(f"    subgraph {tgt_id}[{target_table}]")
    tgt_cols = list(dict.fromkeys(c.get("target_col", "") for c in col_lineage))
    for col in tgt_cols[:10]:
        col_id = _safe_id(f"{target_table}_{col}")
        lines.append(f'        {col_id}["{col}"]')
    lines.append("    end")

    # 엣지
    for c in col_lineage[:max_cols]:
        src_tbl = c.get("source_table", "").upper() or (source_tables[0].upper() if source_tables else "SRC")
        src_col = c.get("source_col", "")
        tgt_col = c.get("target_col", "")
        transform = c.get("transform", "")

        src_id = _safe_id(f"{src_tbl}_{src_col}")
        tgt_id_col = _safe_id(f"{target_table}_{tgt_col}")

        if transform and transform != "LITERAL":
            # 변환 있음 → 중간 노드
            mid_id = _safe_id(f"T_{src_tbl}_{src_col}_{tgt_col}")
            short_transform = transform[:30].replace('"', "'")
            lines.append(f'    {mid_id}{{"{short_transform}"}}')
            lines.append(f"    {src_id} --> {mid_id}")
            lines.append(f"    {mid_id} --> {tgt_id_col}")
        else:
            lines.append(f"    {src_id} --> {tgt_id_col}")

    return "\n".join(lines)


def _safe_id(name: str) -> str:
    """Mermaid 노드 ID로 안전한 문자열 반환"""
    return re.sub(r"[^A-Za-z0-9_]", "_", name.upper())


def generate_mermaid_table_lineage(lineage: dict) -> str:
    """테이블 레벨 리니지 (단순 버전)"""
    lines = ["flowchart LR"]
    seen_nodes = set()

    for rel in lineage.get("table_lineage", []):
        src = rel["from"]
        tgt = rel["to"]
        src_id = _safe_id(src)
        tgt_id = _safe_id(tgt)
        if src_id not in seen_nodes:
            lines.append(f'    {src_id}["{src}"]')
            seen_nodes.add(src_id)
        if tgt_id not in seen_nodes:
            lines.append(f'    {tgt_id}[("{tgt}")]')
            seen_nodes.add(tgt_id)
        lines.append(f"    {src_id} --> {tgt_id}")

    for cte in lineage.get("ctes", []):
        cte_id = _safe_id(cte)
        lines.append(f'    {cte_id}{{"{cte} (CTE)"}}')
        seen_nodes.add(cte_id)

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# 4. LLM 설명 생성 (파싱 결과를 설명만)
# ─────────────────────────────────────────────────────────────

def explain_lineage(lineage: dict) -> str:
    """
    sqlglot이 추출한 리니지 결과를 LLM으로 한국어 설명합니다.
    ★ LLM은 파싱 결과를 설명하는 역할만, SQL 파싱은 하지 않음
    """
    col_count = len(lineage.get("column_lineage", []))
    src_tables = ", ".join(lineage.get("source_tables", []))
    tgt_table  = lineage.get("target_table", "")
    transforms = [c for c in lineage.get("column_lineage", []) if c.get("transform")]

    prompt = f"""다음 ETL SQL 리니지 분석 결과를 데이터 엔지니어가 이해하기 쉽게 한국어로 설명하세요.
2-4 문장으로 간결하게 작성하세요.

## 리니지 분석 결과 (sqlglot 파싱 결과)
- 소스 테이블: {src_tables or "없음"}
- 타겟 테이블: {tgt_table or "없음"}
- 추출된 컬럼 매핑 수: {col_count}개
- 변환 함수 사용 컬럼: {len(transforms)}개
- CTE 사용: {", ".join(lineage.get("ctes", [])) or "없음"}

## 변환 예시 (상위 3개)
{_format_transforms(transforms[:3])}
"""
    return _call_llm(prompt)


def _format_transforms(transforms: list[dict]) -> str:
    if not transforms:
        return "없음"
    lines = []
    for t in transforms:
        lines.append(f"  - {t.get('source_col','')} → {t.get('target_col','')}: {t.get('transform','')[:50]}")
    return "\n".join(lines)


def _call_llm(prompt: str) -> str:
    from aetl_llm import call_llm
    return call_llm(prompt)
