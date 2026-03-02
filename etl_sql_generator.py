"""
ETL 검증 SQL 자동 생성 엔진

LLM(Gemini)을 활용하여 소스/타겟 테이블 메타데이터 기반으로
데이터 검증 쿼리를 자동 생성합니다.

생성하는 검증 쿼리:
  1. row_count_check   - 건수 비교
  2. pk_missing_check  - PK 누락 검증 (소스에 있고 타겟에 없는 것)
  3. null_check        - 주요 컬럼 NULL 체크
  4. duplicate_check   - 타겟 테이블 PK 중복 검증
  5. checksum_check    - 체크섬 비교
  6. full_diff_check   - 전체 데이터 불일치 확인
"""

import json
import os
import re
import textwrap
from typing import Optional

from dotenv import load_dotenv

load_dotenv(override=True)


# ─────────────────────────────────────────
# LLM 초기화
# ─────────────────────────────────────────

def _get_llm():
    """LLM 인스턴스 반환 (LLM_PROVIDER 환경변수로 프로바이더 선택 가능)"""
    from aetl_llm import get_llm
    return get_llm()


# ─────────────────────────────────────────
# 메타데이터 포맷 헬퍼
# ─────────────────────────────────────────

def _format_columns(meta: dict) -> str:
    """컬럼 목록을 프롬프트용 텍스트로 변환"""
    lines = []
    for col in meta.get("columns", []):
        pk_mark = " [PK]" if col.get("pk") else ""
        nn_mark = " NOT NULL" if not col.get("nullable", True) else ""
        type_str = f" {col['type']}" if col.get("type") else ""
        desc = f"  -- {col['description']}" if col.get("description") else ""
        lines.append(f"  {col['name']}{type_str}{pk_mark}{nn_mark}{desc}")
    return "\n".join(lines)


def _format_mapping(mapping: list[dict]) -> str:
    """컬럼 매핑을 프롬프트용 텍스트로 변환"""
    if not mapping:
        return "  (매핑 정보 없음 - 동일 컬럼명 가정)"
    lines = []
    for m in mapping:
        transform = f" → 변환: {m['transform']}" if m.get("transform") else ""
        lines.append(f"  {m['source_col']} → {m['target_col']}{transform}")
    return "\n".join(lines)


# ─────────────────────────────────────────
# 프롬프트 템플릿
# ─────────────────────────────────────────

_GENERATION_PROMPT = """당신은 ETL 데이터 검증 전문가입니다.
소스 테이블에서 타겟 테이블로 ETL 적재 후 데이터 품질을 검증하는 SQL 쿼리를 생성해주세요.

## 소스 테이블: {source_table}
{source_columns}
PK: {source_pk}

## 타겟 테이블: {target_table}
{target_columns}
PK: {target_pk}

## 컬럼 매핑
{column_mapping}

## DB 종류: {db_type}
{db_notes}

## 생성 규칙
1. 실제 실행 가능한 SQL만 생성 (문법 오류 없을 것)
2. 각 쿼리에 한국어 주석 포함
3. {db_type} 문법 사용
4. 컬럼 매핑이 있으면 매핑된 컬럼명으로 비교
5. PK가 없으면 모든 컬럼을 비교 대상으로 사용
6. 체크섬(해시) 생성 시 반드시 컬럼 사이에 '|' 구분자를 삽입하여 해시 충돌을 방지하세요.
7. UNION ALL 내부 각 분기에 행 제한이 필요한 경우, 반드시 서브쿼리로 감싸세요.

## 출력 형식 (반드시 JSON만 출력, 마크다운 코드블록 없이)
{{
  "row_count_check": {{
    "description": "소스/타겟 건수 비교",
    "sql": "SELECT '소스' AS 구분, COUNT(*) AS 건수 FROM {source_table}\\nUNION ALL\\nSELECT '타겟' AS 구분, COUNT(*) AS 건수 FROM {target_table};"
  }},
  "pk_missing_check": {{
    "description": "소스에 있지만 타겟에 없는 PK",
    "sql": "..."
  }},
  "null_check": {{
    "description": "주요 컬럼 NULL 건수 확인",
    "sql": "..."
  }},
  "duplicate_check": {{
    "description": "타겟 PK 중복 검증",
    "sql": "..."
  }},
  "checksum_check": {{
    "description": "데이터 체크섬 비교",
    "sql": "..."
  }},
  "full_diff_check": {{
    "description": "소스/타겟 데이터 불일치 확인",
    "sql": "..."
  }}
}}"""

_DB_NOTES = {
    "oracle": textwrap.dedent("""
    Oracle 전용 주의사항:
    - EXCEPT 대신 MINUS 사용
    - 행 제한: FETCH FIRST N ROWS ONLY
    - 체크섬: ORA_HASH() 또는 STANDARD_HASH() 사용
    - 문자열 연결: || 사용
    - NULL 비교: NVL() 또는 DECODE() 사용
    - 현재 날짜: SYSDATE
    """).strip(),

    "mariadb": textwrap.dedent("""
    MariaDB/MySQL 전용 주의사항:
    - EXCEPT 문법 사용 가능 (MariaDB 10.3+)
    - 행 제한: LIMIT N
    - 체크섬: MD5() 또는 CRC32() 사용
    - 문자열 연결: CONCAT() 사용
    - NULL 비교: IFNULL() 또는 COALESCE() 사용
    - 현재 날짜: NOW()
    """).strip(),

    "postgresql": textwrap.dedent("""
    PostgreSQL 전용 주의사항:
    - EXCEPT 사용 가능 (MINUS 사용 금지!)
    - 행 제한: LIMIT N
    - [중요] UNION ALL과 LIMIT을 함께 사용할 때:
      - UNION ALL 각 분기 안에 LIMIT을 직접 넣으면 문법 오류 발생!
      - 반드시 서브쿼리로 감싸거나 CTE + 최종 LIMIT 패턴 사용:
        올바른 예시 1 (서브쿼리):
          SELECT * FROM (SELECT ... LIMIT 10) s
          UNION ALL
          SELECT * FROM (SELECT ... LIMIT 10) t
        올바른 예시 2 (CTE + 최종 LIMIT):
          WITH cte AS (SELECT ... EXCEPT SELECT ...)
          SELECT * FROM cte LIMIT 10
    - 체크섬: MD5()::text 사용, 반드시 컬럼 사이에 '|' 구분자 삽입
      예: MD5(col1::text || '|' || col2::text)
    - 문자열 연결: || 또는 CONCAT() 사용
    - NULL 비교: COALESCE() 사용
    - 현재 날짜: NOW()
    """).strip(),
}


# ─────────────────────────────────────────
# 핵심 생성 함수
# ─────────────────────────────────────────

def generate_validation_queries(
    source_meta: dict,
    target_meta: dict,
    column_mapping: list[dict] | None = None,
    db_type: str = "oracle",
    llm=None,
) -> dict:
    """
    LLM을 사용하여 ETL 검증 SQL 쿼리 생성

    Args:
        source_meta:    소스 테이블 메타데이터 (etl_metadata_parser 반환값)
        target_meta:    타겟 테이블 메타데이터
        column_mapping: 컬럼 매핑 목록 (없으면 동일 컬럼명 가정)
        db_type:        'oracle' | 'mariadb' | 'postgresql'
        llm:            LLM 인스턴스 (None이면 자동 생성)

    Returns:
        {
          "row_count_check":  {"description": ..., "sql": ...},
          "pk_missing_check": {"description": ..., "sql": ...},
          ...
        }
    """
    if llm is None:
        llm = _get_llm()

    source_table = source_meta["table_name"]
    target_table = target_meta["table_name"]
    source_pk = ", ".join(source_meta.get("pk_columns", [])) or "(PK 없음)"
    target_pk = ", ".join(target_meta.get("pk_columns", [])) or "(PK 없음)"

    prompt = _GENERATION_PROMPT.format(
        source_table=source_table,
        source_columns=_format_columns(source_meta),
        source_pk=source_pk,
        target_table=target_table,
        target_columns=_format_columns(target_meta),
        target_pk=target_pk,
        column_mapping=_format_mapping(column_mapping or []),
        db_type=db_type.upper(),
        db_notes=_DB_NOTES.get(db_type.lower(), ""),
    )

    response = llm.invoke(prompt)
    raw = response.content if hasattr(response, "content") else str(response)

    return _parse_llm_response(raw, source_meta, target_meta, db_type, column_mapping)


def _parse_llm_response(
    raw: str,
    source_meta: dict,
    target_meta: dict,
    db_type: str,
    column_mapping: list[dict] | None,
) -> dict:
    """LLM 응답에서 JSON 파싱, 실패 시 rule-based 폴백"""
    # JSON 블록 추출
    text = raw.strip()
    json_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if json_match:
        text = json_match.group(1).strip()

    # 순수 JSON 시작점 찾기
    brace_start = text.find("{")
    brace_end = text.rfind("}")
    if brace_start != -1 and brace_end != -1:
        text = text[brace_start:brace_end + 1]

    try:
        result = json.loads(text)
        # 각 항목에 sql 키 확인
        expected_keys = [
            "row_count_check", "pk_missing_check", "null_check",
            "duplicate_check", "checksum_check", "full_diff_check",
        ]
        for k in expected_keys:
            if k not in result:
                result[k] = {"description": k, "sql": "-- LLM이 생성하지 않은 쿼리"}
        return _post_validate_sql(result, db_type)
    except json.JSONDecodeError:
        # 파싱 실패 시 rule-based 폴백
        return _generate_fallback_queries(source_meta, target_meta, db_type, column_mapping)


# ─────────────────────────────────────────
# AI 생성 SQL 후처리 검증
# ─────────────────────────────────────────

def _post_validate_sql(queries: dict, db_type: str) -> dict:
    """AI가 생성한 SQL에 대한 후처리 검증 및 자동 치환"""
    is_oracle = db_type.lower() == "oracle"
    is_postgres = db_type.lower() in ("postgresql", "postgres")

    for key, item in queries.items():
        sql = item.get("sql", "")
        if not sql:
            continue

        if is_postgres:
            # MINUS → EXCEPT 치환
            sql = re.sub(r'\bMINUS\b', 'EXCEPT', sql, flags=re.IGNORECASE)
            # UNION ALL 중간의 bare LIMIT → 서브쿼리로 래핑
            sql = _wrap_limit_in_union(sql)
        elif is_oracle:
            # LIMIT N → FETCH FIRST N ROWS ONLY 치환
            sql = re.sub(
                r'\bLIMIT\s+(\d+)\b',
                r'FETCH FIRST \1 ROWS ONLY',
                sql, flags=re.IGNORECASE
            )
            # EXCEPT → MINUS 치환
            sql = re.sub(r'\bEXCEPT\b', 'MINUS', sql, flags=re.IGNORECASE)

        item["sql"] = sql

    return queries


def _wrap_limit_in_union(sql: str) -> str:
    """UNION ALL 중간의 각 분기에 bare LIMIT이 있으면 서브쿼리로 래핑"""
    # UNION ALL이 없으면 그대로 반환
    if not re.search(r'\bUNION\s+ALL\b', sql, re.IGNORECASE):
        return sql

    parts = re.split(r'(\bUNION\s+ALL\b)', sql, flags=re.IGNORECASE)
    result = []
    for part in parts:
        stripped = part.strip()
        # UNION ALL 키워드 자체는 그대로 유지
        if re.match(r'\bUNION\s+ALL\b', stripped, re.IGNORECASE):
            result.append(part)
        # SELECT 분기에 LIMIT이 있으면 서브쿼리로 래핑
        elif re.search(r'\bLIMIT\s+\d+', stripped, re.IGNORECASE) and \
             re.match(r'\s*SELECT\b', stripped, re.IGNORECASE) and \
             not stripped.lstrip().startswith('('):
            # 끝의 세미콜론 분리
            clean = stripped.rstrip(';').strip()
            had_semi = stripped.rstrip().endswith(';')
            wrapped = f"SELECT * FROM ({clean}) _sub"
            if had_semi:
                wrapped += ';'
            result.append(wrapped)
        else:
            result.append(part)
    return '\n'.join(result)


# ─────────────────────────────────────────
# Rule-based 폴백 (LLM 없이도 기본 쿼리 생성)
# ─────────────────────────────────────────

def _generate_fallback_queries(
    source_meta: dict,
    target_meta: dict,
    db_type: str,
    column_mapping: list[dict] | None,
) -> dict:
    """LLM 실패 시 템플릿 기반으로 검증 쿼리 생성"""
    src = source_meta["table_name"]
    tgt = target_meta["table_name"]
    src_pk = source_meta.get("pk_columns", [])
    tgt_pk = target_meta.get("pk_columns", [])

    # 매핑: source_col → target_col
    col_map = {}
    if column_mapping:
        for m in column_mapping:
            if m.get("source_col") and m.get("target_col"):
                col_map[m["source_col"]] = m["target_col"]

    # DB 종류별 함수
    is_oracle = db_type.lower() == "oracle"
    is_postgres = db_type.lower() in ("postgresql", "postgres")
    except_kw = "MINUS" if is_oracle else "EXCEPT"
    limit_clause = "FETCH FIRST 100 ROWS ONLY" if is_oracle else "LIMIT 100"

    # ── 1. row_count_check ──
    row_count_sql = f"""-- 소스/타겟 건수 비교
SELECT '소스({src})' AS 구분, COUNT(*) AS 건수 FROM {src}
UNION ALL
SELECT '타겟({tgt})' AS 구분, COUNT(*) AS 건수 FROM {tgt};"""

    # ── 2. pk_missing_check (Source EXCEPT Target) ──
    if src_pk:
        pk_select_src = ", ".join(src_pk)
        pk_select_tgt = ", ".join(col_map.get(p, p) for p in src_pk)
        if is_postgres:
            pk_missing_sql = f"""-- 소스에 있지만 타겟에 없는 PK (PostgreSQL CTE 방식)
WITH src_data AS (SELECT {pk_select_src} FROM {src}),
     tgt_data AS (SELECT {pk_select_tgt} FROM {tgt}),
     missing AS (
         SELECT * FROM src_data
         EXCEPT
         SELECT * FROM tgt_data
     )
SELECT * FROM missing
{limit_clause};"""
        else:
            pk_missing_sql = f"""-- 소스에 있지만 타겟에 없는 PK
SELECT {pk_select_src}
FROM {src}
{except_kw}
SELECT {pk_select_tgt}
FROM {tgt};"""
    else:
        pk_missing_sql = f"-- PK 정보가 없어 건너뜁니다.\n-- SELECT * FROM {src} MINUS SELECT * FROM {tgt};"

    # ── 3. null_check ──
    # NOT NULL 또는 PK 컬럼에 대해 NULL 체크
    key_cols_src = [c["name"] for c in source_meta["columns"] if c.get("pk") or not c.get("nullable", True)][:5]
    key_cols_tgt = [col_map.get(c, c) for c in key_cols_src]
    if not key_cols_src:
        key_cols_src = [source_meta["columns"][0]["name"]] if source_meta["columns"] else ["*"]
        key_cols_tgt = [col_map.get(key_cols_src[0], key_cols_src[0])]

    null_parts_src = "\n  OR ".join([f"{c} IS NULL" for c in key_cols_src])
    null_parts_tgt = "\n  OR ".join([f"{c} IS NULL" for c in key_cols_tgt])

    null_check_sql = f"""-- 소스 주요 컬럼 NULL 건수 확인
SELECT
  {chr(10).join([f"  SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_NULL_CNT," for c in key_cols_src]).rstrip(',')}
FROM {src};

-- 타겟 주요 컬럼 NULL 건수 확인
SELECT
  {chr(10).join([f"  SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_NULL_CNT," for c in key_cols_tgt]).rstrip(',')}
FROM {tgt};"""

    # ── 4. duplicate_check ──
    if tgt_pk:
        pk_list = ", ".join(tgt_pk)
        duplicate_sql = f"""-- 타겟 테이블 PK 중복 검증
SELECT {pk_list}, COUNT(*) AS CNT
FROM {tgt}
GROUP BY {pk_list}
HAVING COUNT(*) > 1;"""
    else:
        duplicate_sql = f"-- PK 정보가 없어 전체 중복 체크\nSELECT COUNT(*) AS TOTAL, COUNT(DISTINCT ROWID) AS DISTINCT_CNT FROM {tgt};" if is_oracle \
            else f"-- PK 정보가 없어 전체 중복 체크\nSELECT COUNT(*) AS TOTAL FROM {tgt};"

    # ── 5. checksum_check ──
    # 비교 가능한 컬럼만 선택 (매핑 고려)
    src_cols_for_hash = [c["name"] for c in source_meta["columns"] if c.get("pk")][:5]
    tgt_cols_for_hash = [col_map.get(c, c) for c in src_cols_for_hash]
    if not src_cols_for_hash:
        src_cols_for_hash = [c["name"] for c in source_meta["columns"]][:5]
        tgt_cols_for_hash = [col_map.get(c, c) for c in src_cols_for_hash]

    if is_oracle:
        src_hash_expr = " || '|' || ".join([f"NVL(TO_CHAR({c}),'NULL')" for c in src_cols_for_hash])
        tgt_hash_expr = " || '|' || ".join([f"NVL(TO_CHAR({c}),'NULL')" for c in tgt_cols_for_hash])
        checksum_sql = f"""-- 소스/타겟 체크섬 비교
SELECT '소스' AS 구분, SUM(ORA_HASH({src_hash_expr})) AS CHECKSUM FROM {src}
UNION ALL
SELECT '타겟' AS 구분, SUM(ORA_HASH({tgt_hash_expr})) AS CHECKSUM FROM {tgt};"""
    elif is_postgres:
        src_concat = " || '|' || ".join([f"COALESCE(CAST({c} AS TEXT),'NULL')" for c in src_cols_for_hash])
        tgt_concat = " || '|' || ".join([f"COALESCE(CAST({c} AS TEXT),'NULL')" for c in tgt_cols_for_hash])
        checksum_sql = f"""-- 소스/타겟 체크섬 비교
SELECT '소스' AS 구분, SUM(('x' || LEFT(MD5({src_concat}), 8))::bit(32)::int) AS CHECKSUM FROM {src}
UNION ALL
SELECT '타겟' AS 구분, SUM(('x' || LEFT(MD5({tgt_concat}), 8))::bit(32)::int) AS CHECKSUM FROM {tgt};"""
    else:
        src_concat = ", '|', ".join([f"IFNULL(CAST({c} AS CHAR),'NULL')" for c in src_cols_for_hash])
        tgt_concat = ", '|', ".join([f"IFNULL(CAST({c} AS CHAR),'NULL')" for c in tgt_cols_for_hash])
        checksum_sql = f"""-- 소스/타겟 체크섬 비교
SELECT '소스' AS 구분, SUM(CRC32(CONCAT({src_concat}))) AS CHECKSUM FROM {src}
UNION ALL
SELECT '타겟' AS 구분, SUM(CRC32(CONCAT({tgt_concat}))) AS CHECKSUM FROM {tgt};"""

    # ── 6. full_diff_check ──
    if src_pk and tgt_pk:
        src_pk_str = ", ".join([f"S.{p}" for p in src_pk])
        join_cond = " AND ".join([f"S.{sp} = T.{tp}" for sp, tp in zip(src_pk, tgt_pk)])

        # 비교 컬럼 (PK 제외, 최대 10개)
        compare_cols_src = [c["name"] for c in source_meta["columns"] if c["name"] not in src_pk][:10]
        compare_cols_tgt = [col_map.get(c, c) for c in compare_cols_src]

        diff_conditions = []
        for sc, tc in zip(compare_cols_src, compare_cols_tgt):
            if is_oracle:
                diff_conditions.append(f"      DECODE(S.{sc}, T.{tc}, 0, 1) = 1")
            else:
                diff_conditions.append(f"      (S.{sc} <> T.{tc} OR (S.{sc} IS NULL) <> (T.{tc} IS NULL))")

        diff_cond_str = "\n   OR ".join(diff_conditions) if diff_conditions else "1=0 -- 비교할 컬럼 없음"

        full_diff_sql = f"""-- 소스/타겟 데이터 불일치 확인
SELECT
  {src_pk_str},
  'MISMATCH' AS STATUS
FROM {src} S
JOIN {tgt} T ON {join_cond}
WHERE
   {diff_cond_str}
{limit_clause};"""
    else:
        if is_postgres:
            half_limit = 50
            full_diff_sql = f"""-- PK 없음: 데이터 불일치 확인 (PostgreSQL bidirectional CTE)
WITH source_only AS (
    SELECT * FROM {src}
    EXCEPT
    SELECT * FROM {tgt}
),
target_only AS (
    SELECT * FROM {tgt}
    EXCEPT
    SELECT * FROM {src}
)
SELECT * FROM (SELECT '소스에만 존재' AS diff_type, * FROM source_only LIMIT {half_limit}) s
UNION ALL
SELECT * FROM (SELECT '타겟에만 존재' AS diff_type, * FROM target_only LIMIT {half_limit}) t;"""
        else:
            full_diff_sql = f"""-- PK 없음: 전체 소스-타겟 차이 확인
-- 소스에만 있는 데이터
SELECT * FROM {src}
{except_kw}
SELECT * FROM {tgt};"""

    return {
        "row_count_check": {
            "description": "소스/타겟 건수 비교",
            "sql": row_count_sql,
        },
        "pk_missing_check": {
            "description": "소스에 있지만 타겟에 없는 PK",
            "sql": pk_missing_sql,
        },
        "null_check": {
            "description": "주요 컬럼 NULL 건수 확인",
            "sql": null_check_sql,
        },
        "duplicate_check": {
            "description": "타겟 테이블 PK 중복 검증",
            "sql": duplicate_sql,
        },
        "checksum_check": {
            "description": "소스/타겟 체크섬 비교",
            "sql": checksum_sql,
        },
        "full_diff_check": {
            "description": "소스/타겟 데이터 불일치 확인",
            "sql": full_diff_sql,
        },
    }


def generate_validation_queries_no_llm(
    source_meta: dict,
    target_meta: dict,
    column_mapping: list[dict] | None = None,
    db_type: str = "oracle",
) -> dict:
    """LLM 없이 Rule-based로만 검증 쿼리 생성 (빠른 결과, 낮은 품질)"""
    return _generate_fallback_queries(source_meta, target_meta, db_type, column_mapping)


# ─────────────────────────────────────────
# 추가 유틸리티
# ─────────────────────────────────────────

QUERY_LABELS = {
    "row_count_check":  "건수 비교",
    "pk_missing_check": "PK 누락 검증",
    "null_check":       "NULL 체크",
    "duplicate_check":  "중복 검증",
    "checksum_check":   "체크섬 비교",
    "full_diff_check":  "전체 데이터 비교",
}

QUERY_ICONS = {
    "row_count_check":  "📊",
    "pk_missing_check": "🔍",
    "null_check":       "⚠️",
    "duplicate_check":  "🔄",
    "checksum_check":   "🔐",
    "full_diff_check":  "⚖️",
}


# ─────────────────────────────────────────
# AI Rule Suggester  (aetl_profiler 프로파일 기반)
# ─────────────────────────────────────────

def suggest_validation_rules(
    source_profile: dict,
    target_profile: dict | None = None,
    db_type: str = "oracle",
) -> list[dict]:
    """
    데이터 프로파일(aetl_profiler.profile_table 결과)을 분석하여
    Tier 1~3 검증 규칙을 자동 제안합니다.

    Args:
        source_profile: aetl_profiler.profile_table() 반환값
        target_profile: 타겟 프로파일 (없으면 소스 기반만 생성)
        db_type:        'oracle' | 'mariadb' | 'postgresql'

    Returns:
        [
          {
            "rule_name": str,
            "rule_type": str,       # null_check | range_check | uniqueness_check | ...
            "tier": int,            # 1:기술검증 2:정합성 3:비즈니스
            "severity": str,        # CRITICAL | WARNING | INFO
            "source_table": str,
            "target_table": str | None,
            "target_column": str | None,
            "sql": str,             # 실행 가능한 검증 SQL
            "reason": str,          # 자동 생성 근거
            "auto_generated": bool,
          }
        ]
    """
    is_oracle = db_type.lower() == "oracle"
    is_postgres = db_type.lower() in ("postgresql", "postgres")
    src_tbl   = source_profile["table_name"]
    tgt_tbl   = target_profile["table_name"] if target_profile else None
    rules: list[dict] = []

    # ── Tier 2: 건수 비교 (항상 생성) ──
    if tgt_tbl:
        rules.append({
            "rule_name":    f"{src_tbl}_vs_{tgt_tbl}_ROW_COUNT",
            "rule_type":    "row_count_match",
            "tier":         2,
            "severity":     "CRITICAL",
            "source_table": src_tbl,
            "target_table": tgt_tbl,
            "target_column": None,
            "sql": (
                f"SELECT ABS(s.cnt - t.cnt) AS diff\n"
                f"FROM (SELECT COUNT(*) AS cnt FROM {src_tbl}) s,\n"
                f"     (SELECT COUNT(*) AS cnt FROM {tgt_tbl}) t\n"
                f"-- PASS 조건: diff = 0"
            ),
            "reason": "소스·타겟 전체 건수가 반드시 일치해야 합니다.",
            "auto_generated": True,
        })

    for col in source_profile.get("columns", []):
        cname   = col["name"]
        ctype   = col.get("type", "")
        domain  = col.get("inferred_domain", "unknown")
        null_p  = col.get("null_pct", 0.0)
        dist_c  = col.get("distinct_count", 0)
        row_c   = source_profile.get("row_count", 0)
        min_v   = col.get("min")
        max_v   = col.get("max")

        # ── Tier 1: NULL 체크 ──
        # NOT NULL 이어야 하는 컬럼에 NULL이 존재하는 경우
        if null_p == 0.0 and domain in ("id", "code", "amount", "date"):
            severity = "CRITICAL" if domain in ("id",) else "WARNING"
            sql = (
                f"SELECT COUNT(*) AS null_cnt\n"
                f"FROM {src_tbl}\n"
                f"WHERE {cname} IS NULL\n"
                f"-- PASS 조건: null_cnt = 0"
            )
            rules.append({
                "rule_name":     f"{src_tbl}_{cname}_NULL_CHECK",
                "rule_type":     "null_check",
                "tier":          1,
                "severity":      severity,
                "source_table":  src_tbl,
                "target_table":  tgt_tbl,
                "target_column": cname,
                "sql":           sql,
                "reason":        f"'{cname}'은 현재 NULL이 0건이므로 NOT NULL 조건 적용 권장.",
                "auto_generated": True,
            })

        # NULL 비율이 높은 컬럼에 대한 임계치 체크
        if null_p > 0.3:
            sql = (
                f"SELECT ROUND(SUM(CASE WHEN {cname} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS null_pct\n"
                f"FROM {src_tbl}\n"
                f"-- PASS 조건: null_pct <= {round(null_p * 100 + 10, 1)} (현재 기준 +10%)"
            )
            rules.append({
                "rule_name":     f"{src_tbl}_{cname}_NULL_PCT_CHECK",
                "rule_type":     "null_check",
                "tier":          1,
                "severity":      "WARNING",
                "source_table":  src_tbl,
                "target_table":  tgt_tbl,
                "target_column": cname,
                "sql":           sql,
                "threshold":     round(null_p * 100 + 10, 1),
                "reason":        f"'{cname}' NULL 비율이 {round(null_p*100,1)}%로 높습니다. 임계치 모니터링 권장.",
                "auto_generated": True,
            })

        # ── Tier 1: 유일성(PK 후보) 체크 ──
        if row_c > 0 and dist_c == row_c and dist_c > 100:
            sql = (
                f"SELECT {cname}, COUNT(*) AS dup_cnt\n"
                f"FROM {src_tbl}\n"
                f"GROUP BY {cname}\n"
                f"HAVING COUNT(*) > 1\n"
                f"-- PASS 조건: 0건"
            )
            rules.append({
                "rule_name":     f"{src_tbl}_{cname}_UNIQUENESS_CHECK",
                "rule_type":     "uniqueness_check",
                "tier":          1,
                "severity":      "CRITICAL",
                "source_table":  src_tbl,
                "target_table":  tgt_tbl,
                "target_column": cname,
                "sql":           sql,
                "reason":        f"'{cname}' distinct_count={dist_c} = row_count 이므로 PK 후보 컬럼입니다.",
                "auto_generated": True,
            })

        # ── Tier 3: 비즈니스 규칙 — 금액 양수 체크 ──
        if domain == "amount" and min_v is not None:
            try:
                min_num = float(min_v)
                if min_num >= 0:
                    sql = (
                        f"SELECT COUNT(*) AS negative_cnt\n"
                        f"FROM {src_tbl}\n"
                        f"WHERE {cname} < 0\n"
                        f"-- PASS 조건: negative_cnt = 0"
                    )
                    rules.append({
                        "rule_name":     f"{src_tbl}_{cname}_POSITIVE_CHECK",
                        "rule_type":     "range_check",
                        "tier":          3,
                        "severity":      "WARNING",
                        "source_table":  src_tbl,
                        "target_table":  tgt_tbl,
                        "target_column": cname,
                        "sql":           sql,
                        "reason":        f"'{cname}'은 금액 계열 컬럼으로 음수 불가 조건 적용 권장.",
                        "auto_generated": True,
                    })
            except (ValueError, TypeError):
                pass

        # ── Tier 3: 날짜 범위 드리프트 체크 ──
        if domain == "date" and min_v and max_v:
            if is_oracle:
                sql = (
                    f"SELECT COUNT(*) AS future_cnt\n"
                    f"FROM {src_tbl}\n"
                    f"WHERE {cname} > SYSDATE + 1\n"
                    f"-- PASS 조건: future_cnt = 0"
                )
            elif is_postgres:
                sql = (
                    f"SELECT COUNT(*) AS future_cnt\n"
                    f"FROM {src_tbl}\n"
                    f"WHERE {cname} > NOW() + INTERVAL '1 day'\n"
                    f"-- PASS 조건: future_cnt = 0"
                )
            else:
                sql = (
                    f"SELECT COUNT(*) AS future_cnt\n"
                    f"FROM {src_tbl}\n"
                    f"WHERE {cname} > DATE_ADD(NOW(), INTERVAL 1 DAY)\n"
                    f"-- PASS 조건: future_cnt = 0"
                )
            rules.append({
                "rule_name":     f"{src_tbl}_{cname}_FUTURE_DATE_CHECK",
                "rule_type":     "range_check",
                "tier":          3,
                "severity":      "INFO",
                "source_table":  src_tbl,
                "target_table":  tgt_tbl,
                "target_column": cname,
                "sql":           sql,
                "reason":        f"'{cname}'은 날짜 컬럼입니다. 미래 날짜 존재 여부 확인 권장.",
                "auto_generated": True,
            })

        # ── Tier 2: 타겟과 컬럼별 합계 비교 (금액·수량) ──
        if tgt_tbl and domain in ("amount", "count"):
            null_fn = "NVL" if is_oracle else "COALESCE" if is_postgres else "IFNULL"
            sql = (
                f"SELECT\n"
                f"  ABS(s.total - t.total) AS diff,\n"
                f"  s.total AS src_total,\n"
                f"  t.total AS tgt_total\n"
                f"FROM (SELECT SUM({null_fn}({cname},0)) AS total FROM {src_tbl}) s,\n"
                f"     (SELECT SUM({null_fn}({cname},0)) AS total FROM {tgt_tbl}) t\n"
                f"-- PASS 조건: diff = 0"
            )
            rules.append({
                "rule_name":     f"{src_tbl}_{cname}_SUM_MATCH",
                "rule_type":     "sum_match",
                "tier":          2,
                "severity":      "CRITICAL",
                "source_table":  src_tbl,
                "target_table":  tgt_tbl,
                "target_column": cname,
                "sql":           sql,
                "reason":        f"'{cname}'은 {domain} 컬럼 — 소스·타겟 합계 일치 필수.",
                "auto_generated": True,
            })

    return rules
