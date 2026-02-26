"""
================================================================================
AETL Execution Engine  —  v2.1
================================================================================
원칙: Human-in-the-Loop
  - SELECT  → 자동 실행 (안전)
  - DML/DDL → 사용자가 UI에서 [실행] 버튼 클릭 후에만 실행
  - Auto-Fix 루프 없음 — AI는 수정 SQL '제안'만, 실행은 사람이 결정
================================================================================
"""

from __future__ import annotations

import json
import time
from typing import Any

import sqlglot
from sqlglot import exp


# ─────────────────────────────────────────────────────────────
# SQL 분류기 (sqlglot 규칙 기반)
# ─────────────────────────────────────────────────────────────

_DIALECT_MAP = {"oracle": "oracle", "mariadb": "mysql", "postgresql": "postgres"}


def classify_sql(sql: str, db_type: str = "oracle") -> str:
    """
    SQL 구문을 분류합니다 (sqlglot AST 기반).
    Returns: "SELECT" | "DML" | "DDL" | "UNKNOWN"
    """
    dialect = _DIALECT_MAP.get(db_type, "ansi")
    try:
        parsed = sqlglot.parse_one(sql.strip(), dialect=dialect)
        if isinstance(parsed, exp.Select):
            return "SELECT"
        elif isinstance(parsed, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
            return "DML"
        elif isinstance(parsed, (exp.Create, exp.Alter, exp.Drop, exp.TruncateTable)):
            return "DDL"
        return "UNKNOWN"
    except Exception:
        # fallback: 키워드 기반
        upper = sql.strip().upper().lstrip("(")
        if upper.startswith(("SELECT", "WITH")):
            return "SELECT"
        if upper.startswith(("INSERT", "UPDATE", "DELETE", "MERGE")):
            return "DML"
        if upper.startswith(("CREATE", "ALTER", "DROP", "TRUNCATE")):
            return "DDL"
        return "UNKNOWN"


def _has_dml_in_tree(sql: str, db_type: str) -> bool:
    """
    AST 트리 전체를 탐색하여 서브쿼리 내 DML/DDL 노드 존재 시 True 반환.
    최상위가 SELECT라도 내부에 DELETE/INSERT 등이 있으면 차단.
    """
    dialect = _DIALECT_MAP.get(db_type, "ansi")
    try:
        parsed = sqlglot.parse_one(sql.strip(), dialect=dialect)
        for node in parsed.walk():
            if isinstance(node, (
                exp.Insert, exp.Update, exp.Delete, exp.Merge,
                exp.Create, exp.Alter, exp.Drop, exp.TruncateTable,
            )):
                return True
        return False
    except Exception:
        return False


def is_safe_to_autorun(sql: str, db_type: str = "oracle") -> bool:
    """SELECT 전용이고 서브쿼리에 DML/DDL 없는지 확인"""
    return classify_sql(sql, db_type) == "SELECT" and not _has_dml_in_tree(sql, db_type)


# ─────────────────────────────────────────────────────────────
# DB 연결 헬퍼
# ─────────────────────────────────────────────────────────────

def _get_connection(config: dict):
    """db_config.json 기반으로 DB 커넥션 반환"""
    db_type = config.get("db_type", "oracle").lower()
    conn_cfg = config["connection"]

    if db_type == "oracle":
        import oracledb
        dsn = f"{conn_cfg['host']}:{conn_cfg['port']}/{conn_cfg['database']}"
        return oracledb.connect(
            user=conn_cfg["user"], password=conn_cfg["password"], dsn=dsn
        )
    elif db_type == "mariadb":
        import mariadb
        return mariadb.connect(
            host=conn_cfg["host"], port=int(conn_cfg.get("port", 3306)),
            user=conn_cfg["user"], password=conn_cfg["password"],
            database=conn_cfg["database"],
        )
    elif db_type == "postgresql":
        import psycopg2
        return psycopg2.connect(
            host=conn_cfg["host"], port=int(conn_cfg.get("port", 5432)),
            user=conn_cfg["user"], password=conn_cfg["password"],
            dbname=conn_cfg["database"],
        )
    raise ValueError(f"지원하지 않는 DB 종류: {db_type}")


def _apply_row_limit(sql: str, limit: int, db_type: str) -> str:
    """DB 방언에 맞게 행수 제한 적용"""
    upper = sql.strip().upper()
    # 이미 제한이 있으면 그대로
    if any(kw in upper for kw in ("ROWNUM", "FETCH FIRST", "LIMIT ", "TOP ")):
        return sql

    if db_type == "oracle":
        return f"SELECT * FROM ({sql}) WHERE ROWNUM <= {limit}"
    elif db_type in ("mariadb", "postgresql"):
        return f"{sql.rstrip(';')} LIMIT {limit}"
    return sql


# ─────────────────────────────────────────────────────────────
# 실행 함수
# ─────────────────────────────────────────────────────────────

def execute_query(
    sql: str,
    config_path: str = "db_config.json",
    row_limit: int = 1000,
) -> dict:
    """
    SELECT SQL을 안전하게 실행하고 결과를 반환합니다.

    Returns dict:
      {
        "ok": bool,
        "columns": list[str],
        "rows": list[tuple],
        "row_count": int,
        "elapsed_sec": float,
        "sql_type": str,
        "error": str | None,
      }
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)
    except Exception as e:
        return {"ok": False, "error": f"db_config.json 읽기 실패: {e}",
                "columns": [], "rows": [], "row_count": 0, "elapsed_sec": 0, "sql_type": "?"}

    db_type = config.get("db_type", "oracle").lower()
    sql_type = classify_sql(sql, db_type)

    if sql_type != "SELECT":
        return {
            "ok": False,
            "error": f"안전 실행기는 SELECT만 허용합니다. 현재 SQL 유형: {sql_type}",
            "columns": [], "rows": [], "row_count": 0, "elapsed_sec": 0,
            "sql_type": sql_type,
        }

    try:
        conn = _get_connection(config)
        limited_sql = _apply_row_limit(sql, row_limit, db_type)
        t0 = time.time()
        cur = conn.cursor()
        cur.execute(limited_sql)
        columns = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
        elapsed = round(time.time() - t0, 3)
        conn.close()
        return {
            "ok": True, "columns": columns, "rows": rows,
            "row_count": len(rows), "elapsed_sec": elapsed,
            "sql_type": sql_type, "error": None,
        }
    except Exception as e:
        return {
            "ok": False, "error": str(e),
            "columns": [], "rows": [], "row_count": 0, "elapsed_sec": 0,
            "sql_type": sql_type,
        }


def execute_dml(
    sql: str,
    config_path: str = "db_config.json",
) -> dict:
    """
    DML/DDL SQL을 실행합니다.
    ★ 이 함수는 반드시 사용자 명시적 승인 후에만 호출해야 합니다.

    Returns dict:
      {"ok": bool, "affected_rows": int, "elapsed_sec": float, "error": str|None}
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)
    except Exception as e:
        return {"ok": False, "error": f"설정 파일 오류: {e}", "affected_rows": 0, "elapsed_sec": 0}

    db_type = config.get("db_type", "oracle").lower()
    sql_type = classify_sql(sql, db_type)

    try:
        conn = _get_connection(config)
        t0 = time.time()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        affected = cur.rowcount if cur.rowcount is not None else -1
        elapsed = round(time.time() - t0, 3)
        conn.close()

        # 실행 이력 저장
        _log_execution(sql, sql_type, "SUCCESS", affected, config_path)

        return {"ok": True, "affected_rows": affected, "elapsed_sec": elapsed, "error": None}
    except Exception as e:
        _log_execution(sql, sql_type, "FAIL", 0, config_path, str(e))
        return {"ok": False, "error": str(e), "affected_rows": 0, "elapsed_sec": 0}


# ─────────────────────────────────────────────────────────────
# 실행 이력 로깅 (SQLite)
# ─────────────────────────────────────────────────────────────

def _log_execution(
    sql: str, sql_type: str, status: str,
    affected_rows: int, config_path: str, error: str = ""
):
    try:
        from aetl_store import init_db
        import sqlite3
        db_path = "aetl_metadata.db"
        init_db(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS execution_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sql_type    TEXT,
                sql_text    TEXT,
                status      TEXT,
                affected_rows INTEGER,
                error_msg   TEXT,
                executed_at TEXT DEFAULT (datetime('now','localtime'))
            )
        """)
        conn.execute(
            "INSERT INTO execution_log (sql_type, sql_text, status, affected_rows, error_msg)"
            " VALUES (?, ?, ?, ?, ?)",
            (sql_type, sql[:2000], status, affected_rows, error)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_execution_log(limit: int = 50) -> list[dict]:
    """실행 이력 조회"""
    try:
        import sqlite3
        conn = sqlite3.connect("aetl_metadata.db")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM execution_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────
# AI 진단 (검증 실패 시)
# ─────────────────────────────────────────────────────────────

def diagnose_failure(
    validation_name: str,
    result: dict,
    source_table: str,
    target_table: str,
    db_type: str = "oracle",
) -> dict:
    """
    검증 실패 원인을 AI로 분석하고 수정 SQL을 제안합니다.
    ★ 반환된 fix_sqls는 화면에 표시만 하고, 실행은 사람이 결정합니다.

    Returns:
      {
        "diagnosis": str,        # 원인 분석 설명
        "probing_results": [...], # 근거 SELECT 실행 결과
        "fix_sqls": [            # 수정 SQL 제안 (실행 안 함)
          {"description": str, "sql": str, "strategy": str}
        ]
      }
    """
    import os
    from dotenv import load_dotenv
    load_dotenv()

    # 1. 진단 프롬프트 빌드
    prompt = _build_diagnosis_prompt(validation_name, result, source_table, target_table, db_type)

    # 2. LLM 호출
    raw_response = _call_llm(prompt)

    # 3. 응답 파싱
    return _parse_diagnosis_response(raw_response, source_table, target_table, db_type)


def _build_diagnosis_prompt(
    validation_name: str, result: dict,
    source_table: str, target_table: str, db_type: str
) -> str:
    return f"""당신은 ETL 데이터 품질 전문가입니다.
다음 검증 실패를 분석하고 원인과 수정 방안을 JSON으로 응답하세요.

## 검증 정보
- 검증명: {validation_name}
- 소스 테이블: {source_table}
- 타겟 테이블: {target_table}
- DB 종류: {db_type}
- 실행 결과: {json.dumps(result, ensure_ascii=False, default=str)[:500]}

## 응답 형식 (JSON만 응답)
{{
  "diagnosis": "원인 분석 설명 (2-3문장)",
  "confidence": "HIGH|MEDIUM|LOW",
  "probing_sqls": [
    {{"purpose": "확인 목적", "sql": "SELECT ..."}}
  ],
  "fix_sqls": [
    {{"description": "방안 설명", "strategy": "strategy_key", "sql": "수정 SQL"}}
  ]
}}

규칙:
- probing_sqls는 SELECT만 (실행하여 근거 수집용)
- fix_sqls는 제안만 (사용자가 선택 후 실행)
- confidence가 LOW이면 fix_sqls는 빈 배열로 반환
- 모든 SQL은 {db_type} 방언에 맞게 작성
"""


def _call_llm(prompt: str) -> str:
    import os
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

    return '{"diagnosis": "API 키 없음", "confidence": "LOW", "probing_sqls": [], "fix_sqls": []}'


def _parse_diagnosis_response(raw: str, source_table: str, target_table: str, db_type: str) -> dict:
    import re
    # JSON 블록 추출
    m = re.search(r"\{[\s\S]+\}", raw)
    if not m:
        return {"diagnosis": raw, "probing_results": [], "fix_sqls": []}
    try:
        data = json.loads(m.group())
        # probing SQLs 실행 (SELECT만, 최대 3개)
        probing_results = []
        for ps in data.get("probing_sqls", [])[:3]:
            sql = ps.get("sql", "")
            # 최상위 SELECT 검사 + 서브쿼리 DML/DDL 이중 차단
            if classify_sql(sql, db_type) == "SELECT" and not _has_dml_in_tree(sql, db_type):
                res = execute_query(sql)
                probing_results.append({
                    "purpose": ps.get("purpose", ""),
                    "sql": sql,
                    "result": res,
                })
        return {
            "diagnosis":       data.get("diagnosis", ""),
            "confidence":      data.get("confidence", "LOW"),
            "probing_results": probing_results,
            "fix_sqls":        data.get("fix_sqls", []),
        }
    except json.JSONDecodeError:
        return {"diagnosis": raw[:500], "probing_results": [], "fix_sqls": []}
