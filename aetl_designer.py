"""
================================================================================
AETL DW Designer Engine  —  v2.1
================================================================================
입력 전략 (우선순위):
  1순위: Swagger/OpenAPI JSON/YAML → 완전 자동 파싱
  2순위: Excel/CSV 테이블 정의서  → 기존 parser 재사용
  3순위: PDF 텍스트              → AI 초안만 (사용자 검토 필수)

출력:
  - Star Schema 설계 (JSON)
  - Mermaid erDiagram 코드 (Streamlit 렌더링)
  - st.data_editor용 DataFrame
  - DDL Script
================================================================================
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from dotenv import load_dotenv

load_dotenv()


# ─────────────────────────────────────────────────────────────
# 1. 입력 파서
# ─────────────────────────────────────────────────────────────

def parse_swagger(content: str | bytes) -> dict:
    """
    Swagger/OpenAPI JSON 또는 YAML을 파싱하여 엔티티 구조를 추출합니다.

    Returns:
        {
          "entities": [{"name": str, "fields": [{"name": str, "type": str, "desc": str}]}],
          "source": "swagger"
        }
    """
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")

    # JSON 시도
    spec = None
    try:
        spec = json.loads(content)
    except json.JSONDecodeError:
        # YAML 시도
        try:
            import yaml
            spec = yaml.safe_load(content)
        except Exception:
            pass

    if not spec:
        return {"entities": [], "source": "swagger", "error": "파싱 실패"}

    entities = []
    # OpenAPI 3.x / Swagger 2.x 공통: components/schemas 또는 definitions
    schemas = (
        spec.get("components", {}).get("schemas", {})
        or spec.get("definitions", {})
    )

    for schema_name, schema_body in schemas.items():
        props = schema_body.get("properties", {})
        required = set(schema_body.get("required", []))
        fields = []
        for field_name, field_body in props.items():
            ftype = field_body.get("type", "string")
            if "format" in field_body:
                ftype = f"{ftype}({field_body['format']})"
            fields.append({
                "name":     field_name,
                "type":     ftype,
                "desc":     field_body.get("description", ""),
                "required": field_name in required,
            })
        entities.append({"name": schema_name, "fields": fields})

    return {"entities": entities, "source": "swagger"}


def parse_table_definition_text(text: str) -> dict:
    """
    자유 형식 텍스트(PDF 추출 등)에서 테이블 정의를 AI로 추출합니다.
    ⚠ 초안만 제공 — 반드시 사용자 검토 필요

    Returns:
        {"entities": [...], "source": "text_ai", "warning": str}
    """
    prompt = f"""다음 텍스트에서 데이터베이스 테이블 정의를 추출하세요.
JSON만 응답하세요.

텍스트:
{text[:3000]}

응답 형식:
{{
  "entities": [
    {{
      "name": "엔티티명",
      "fields": [
        {{"name": "필드명", "type": "데이터타입", "desc": "설명", "required": true/false}}
      ]
    }}
  ]
}}
"""
    raw = _call_llm(prompt)
    m = re.search(r"\{[\s\S]+\}", raw)
    if not m:
        return {"entities": [], "source": "text_ai",
                "warning": "⚠ AI가 구조를 추출하지 못했습니다. 직접 입력하세요."}
    try:
        data = json.loads(m.group())
        data["source"] = "text_ai"
        data["warning"] = "⚠ AI 초안입니다. 반드시 내용을 검토하고 수정하세요."
        return data
    except Exception:
        return {"entities": [], "source": "text_ai",
                "warning": "⚠ 파싱 실패. 직접 입력하세요."}


# ─────────────────────────────────────────────────────────────
# 2. Star Schema 설계 (AI)
# ─────────────────────────────────────────────────────────────

def design_star_schema(entities: list[dict], context: str = "") -> dict:
    """
    엔티티 목록을 받아 ODS/DW Star Schema를 설계합니다.

    Returns:
        {
          "ods_tables": [...],
          "fact_tables": [...],
          "dim_tables":  [...],
          "dm_tables":   [...],
          "relationships": [...],
        }
    """
    entities_json = json.dumps(entities, ensure_ascii=False, indent=2)[:3000]

    prompt = f"""당신은 데이터 웨어하우스 아키텍트입니다.
다음 엔티티 구조를 분석하여 3-Layer DW 모델을 설계하세요.

## 엔티티 목록
{entities_json}

## 추가 맥락
{context or "없음"}

## 응답 형식 (JSON만)
{{
  "ods_tables": [
    {{
      "name": "ODS_xxx",
      "comment": "설명",
      "source_entity": "원본 엔티티명",
      "columns": [
        {{"name": "컬럼명", "type": "VARCHAR2(200)", "pk": false, "nullable": true, "desc": "설명"}}
      ]
    }}
  ],
  "fact_tables": [
    {{
      "name": "FACT_xxx",
      "comment": "설명",
      "grain": "행 단위 설명",
      "columns": [...],
      "measures": ["측정값 컬럼명 목록"]
    }}
  ],
  "dim_tables": [
    {{
      "name": "DIM_xxx",
      "comment": "설명",
      "scd_type": 1,
      "natural_key": "자연키 컬럼명",
      "columns": [...]
    }}
  ],
  "dm_tables": [
    {{
      "name": "DM_xxx",
      "comment": "설명",
      "purpose": "분석 목적",
      "columns": [...]
    }}
  ],
  "relationships": [
    {{"from": "FACT_xxx", "to": "DIM_xxx", "fk": "fk_컬럼명", "type": "N:1"}}
  ]
}}

규칙:
- ODS 테이블에는 ETL_DT (적재일시), BATCH_ID 컬럼 추가
- FACT 테이블의 PK는 SK_ 접두사 사용 (대리키)
- DIM 테이블도 SK_ 대리키 + 자연키 구분
- 날짜/시간 관련 → DIM_DATE 자동 추가
- 모든 컬럼 타입은 Oracle 기준
"""
    raw = _call_llm(prompt)
    m = re.search(r"\{[\s\S]+\}", raw)
    if not m:
        return {"ods_tables": [], "fact_tables": [], "dim_tables": [], "dm_tables": [], "relationships": []}
    try:
        return json.loads(m.group())
    except Exception:
        return {"ods_tables": [], "fact_tables": [], "dim_tables": [], "dm_tables": [], "relationships": []}


# ─────────────────────────────────────────────────────────────
# 3. Mermaid ERD 생성
# ─────────────────────────────────────────────────────────────

def generate_mermaid_erd(design: dict, layer: str = "all") -> str:
    """
    Star Schema 설계 결과를 Mermaid erDiagram으로 변환합니다.

    Args:
        design: design_star_schema()의 반환값
        layer:  "ods" | "dw" | "dm" | "all"

    Returns:
        Mermaid erDiagram 코드
    """
    lines = ["erDiagram"]

    tables = []
    if layer in ("ods", "all"):
        tables += [(t, "ODS") for t in design.get("ods_tables", [])]
    if layer in ("dw", "all"):
        tables += [(t, "FACT") for t in design.get("fact_tables", [])]
        tables += [(t, "DIM")  for t in design.get("dim_tables", [])]
    if layer in ("dm", "all"):
        tables += [(t, "DM")   for t in design.get("dm_tables", [])]

    for tbl, ttype in tables:
        name = tbl.get("name", "UNKNOWN")
        cols = tbl.get("columns", [])[:12]  # Mermaid 가독성 위해 최대 12열
        lines.append(f"    {name} {{")
        for col in cols:
            ctype = _mermaid_type(col.get("type", "string"))
            cname = col.get("name", "col")
            pk_flag = "PK" if col.get("pk") else ""
            comment = col.get("desc", "")[:20].replace('"', "")
            if pk_flag:
                lines.append(f'        {ctype} {cname} PK "{comment}"')
            else:
                lines.append(f'        {ctype} {cname} "{comment}"')
        lines.append("    }")

    # 관계선
    for rel in design.get("relationships", []):
        src = rel.get("from", "")
        tgt = rel.get("to", "")
        rel_type = rel.get("type", "N:1")
        fk = rel.get("fk", "")
        mermaid_rel = "||--o{" if rel_type in ("1:N", "1:M") else "}o--||"
        lines.append(f'    {src} {mermaid_rel} {tgt} : "{fk}"')

    return "\n".join(lines)


def _mermaid_type(oracle_type: str) -> str:
    """Oracle 타입을 Mermaid 호환 타입으로 변환"""
    upper = oracle_type.upper().split("(")[0]
    mapping = {
        "VARCHAR2": "string", "VARCHAR": "string", "CHAR": "string",
        "NUMBER": "float", "INTEGER": "int", "BIGINT": "bigint",
        "DATE": "date", "TIMESTAMP": "timestamp",
        "CLOB": "text", "BLOB": "blob",
        "DECIMAL": "float", "NUMERIC": "float",
    }
    return mapping.get(upper, "string")


def generate_mermaid_flow(design: dict) -> str:
    """
    ODS → DW → DM 흐름도를 Mermaid flowchart로 생성합니다.
    """
    lines = ["flowchart LR"]
    lines.append("    subgraph ODS[ODS Layer]")
    for t in design.get("ods_tables", []):
        lines.append(f'        {t["name"]}["{t["name"]}\\n{t.get("comment","")}"]')
    lines.append("    end")

    lines.append("    subgraph DW[DW Layer - Star Schema]")
    for t in design.get("fact_tables", []):
        lines.append(f'        {t["name"]}[/"FACT: {t["name"]}\\n{t.get("comment","")}"/]')
    for t in design.get("dim_tables", []):
        lines.append(f'        {t["name"]}["{t["name"]}\\n{t.get("comment","")}"]')
    lines.append("    end")

    if design.get("dm_tables"):
        lines.append("    subgraph DM[DM Layer]")
        for t in design.get("dm_tables", []):
            lines.append(f'        {t["name"]}[("{t["name"]}\\n{t.get("comment","")}")]')
        lines.append("    end")

    # ODS → DW 연결
    ods_names = [t["name"] for t in design.get("ods_tables", [])]
    fact_names = [t["name"] for t in design.get("fact_tables", [])]
    for ods in ods_names[:3]:
        for fact in fact_names[:2]:
            lines.append(f"    {ods} --> {fact}")

    # DW → DM 연결
    dm_names = [t["name"] for t in design.get("dm_tables", [])]
    for fact in fact_names[:2]:
        for dm in dm_names[:2]:
            lines.append(f"    {fact} --> {dm}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# 4. 설계 → DDL 전체 생성
# ─────────────────────────────────────────────────────────────

def design_to_ddl(design: dict, db_type: str = "oracle") -> str:
    """Star Schema 설계 전체를 DDL 스크립트로 변환합니다."""
    from aetl_export import generate_ddl

    sections = []
    layer_order = [
        ("-- ═══ ODS Layer ═══", design.get("ods_tables", [])),
        ("-- ═══ DW Dimension ═══", design.get("dim_tables", [])),
        ("-- ═══ DW Fact ═══",     design.get("fact_tables", [])),
        ("-- ═══ DM Layer ═══",    design.get("dm_tables", [])),
    ]
    for comment, tables in layer_order:
        if not tables:
            continue
        parts = [comment]
        for tbl in tables:
            pk_cols = [c["name"] for c in tbl.get("columns", []) if c.get("pk")]
            meta = {
                "table_name":  tbl["name"],
                "columns":     tbl.get("columns", []),
                "pk_columns":  pk_cols,
            }
            parts.append(f"-- {tbl.get('comment', '')}")
            parts.append(generate_ddl(meta, db_type))
            parts.append("")
        sections.append("\n".join(parts))

    return "\n\n".join(sections)


# ─────────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────────

def _call_llm(prompt: str) -> str:
    google_key = os.getenv("GOOGLE_API_KEY")
    if google_key:
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0.0)
            return llm.invoke(prompt).content
        except Exception:
            pass

    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    if anthropic_key:
        from langchain_anthropic import ChatAnthropic
        llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0.0)
        return llm.invoke(prompt).content

    return "{}"
