"""
================================================================================
AETL Metadata Store  —  SQLite 기반 영속 저장소
================================================================================
프로파일링 결과, 검증 규칙, 검증 실행 이력을 SQLite에 저장·조회합니다.
DB 파일: aetl_metadata.db (프로젝트 루트)

추후 PostgreSQL 마이그레이션 시 SQL 구문만 교체하면 됩니다.
================================================================================
"""

import json
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).parent / "aetl_metadata.db"


# ─────────────────────────────────────────────────────────────
# 초기화
# ─────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS datasource (
    source_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name     TEXT    NOT NULL UNIQUE,
    db_type         TEXT    NOT NULL,   -- oracle | mariadb | postgresql
    config_path     TEXT,               -- db_config.json 경로
    last_crawled_at TEXT,               -- ISO datetime
    created_at      TEXT    DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS table_meta (
    table_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id   INTEGER REFERENCES datasource(source_id),
    table_name  TEXT    NOT NULL,
    row_count   INTEGER,
    profile_json TEXT,                  -- JSON: aetl_profiler.profile_table() 반환값
    crawled_at  TEXT    DEFAULT (datetime('now','localtime')),
    UNIQUE(source_id, table_name)
);

CREATE TABLE IF NOT EXISTS column_meta (
    column_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id        INTEGER REFERENCES table_meta(table_id),
    column_name     TEXT    NOT NULL,
    data_type       TEXT,
    null_pct        REAL,
    distinct_count  INTEGER,
    min_val         TEXT,
    max_val         TEXT,
    top_values_json TEXT,               -- JSON array
    inferred_domain TEXT,
    UNIQUE(table_id, column_name)
);

CREATE TABLE IF NOT EXISTS validation_rule (
    rule_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_name       TEXT    NOT NULL,
    rule_type       TEXT    NOT NULL,   -- null_check | row_count_match | range_check | ...
    tier            INTEGER,            -- 1:technical 2:reconciliation 3:business
    source_table    TEXT,
    target_table    TEXT,
    target_column   TEXT,
    rule_sql        TEXT,               -- 실행할 SQL 또는 표현식
    severity        TEXT    DEFAULT 'WARNING',  -- CRITICAL | WARNING | INFO
    threshold       REAL,               -- 허용 임계치
    auto_generated  INTEGER DEFAULT 0,  -- 1=AI 자동 생성
    reason          TEXT,               -- 생성 근거
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT    DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS validation_result (
    result_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id    TEXT    NOT NULL,   -- UUID
    rule_id         INTEGER REFERENCES validation_rule(rule_id),
    rule_name       TEXT,               -- 비정규화 (rule 삭제 후 이력 보존)
    run_timestamp   TEXT    DEFAULT (datetime('now','localtime')),
    status          TEXT    NOT NULL,   -- PASS | FAIL | WARN | ERROR
    actual_value    TEXT,
    expected_value  TEXT,
    detail_json     TEXT,               -- JSON
    ai_analysis     TEXT                -- AI 원인 분석 코멘트
);

CREATE INDEX IF NOT EXISTS idx_tbl_source ON table_meta(source_id);
CREATE INDEX IF NOT EXISTS idx_col_table  ON column_meta(table_id);
CREATE INDEX IF NOT EXISTS idx_vr_exec    ON validation_result(execution_id);
CREATE INDEX IF NOT EXISTS idx_vr_rule    ON validation_result(rule_id);
"""


def _conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path = DB_PATH):
    """DB 초기화 (테이블 생성). 이미 존재하면 무시."""
    with _conn(db_path) as conn:
        conn.executescript(_DDL)


# ─────────────────────────────────────────────────────────────
# Datasource CRUD
# ─────────────────────────────────────────────────────────────
def get_or_create_datasource(
    source_name: str,
    db_type: str,
    config_path: str | None = None,
) -> int:
    """
    datasource 레코드를 반환하거나 없으면 생성합니다.
    Returns: source_id
    """
    init_db()
    with _conn() as conn:
        row = conn.execute(
            "SELECT source_id FROM datasource WHERE source_name = ?", (source_name,)
        ).fetchone()
        if row:
            return row["source_id"]
        cur = conn.execute(
            "INSERT INTO datasource (source_name, db_type, config_path) VALUES (?,?,?)",
            (source_name, db_type, config_path),
        )
        return cur.lastrowid


def list_datasources() -> list[dict]:
    init_db()
    with _conn() as conn:
        rows = conn.execute("SELECT * FROM datasource ORDER BY source_id").fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Profile 저장 / 조회
# ─────────────────────────────────────────────────────────────
def save_profile(table_profile: dict, source_id: int) -> int:
    """
    프로파일 결과를 table_meta + column_meta 에 저장(UPSERT).
    Returns: table_id
    """
    init_db()
    table_name   = table_profile["table_name"]
    row_count    = table_profile["row_count"]
    profile_json = json.dumps(table_profile, ensure_ascii=False)

    with _conn() as conn:
        # table_meta upsert
        conn.execute("""
            INSERT INTO table_meta (source_id, table_name, row_count, profile_json, crawled_at)
            VALUES (?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(source_id, table_name)
            DO UPDATE SET row_count=excluded.row_count,
                          profile_json=excluded.profile_json,
                          crawled_at=excluded.crawled_at
        """, (source_id, table_name, row_count, profile_json))

        table_id = conn.execute(
            "SELECT table_id FROM table_meta WHERE source_id=? AND table_name=?",
            (source_id, table_name)
        ).fetchone()["table_id"]

        # column_meta upsert
        for col in table_profile.get("columns", []):
            conn.execute("""
                INSERT INTO column_meta
                    (table_id, column_name, data_type, null_pct, distinct_count,
                     min_val, max_val, top_values_json, inferred_domain)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(table_id, column_name)
                DO UPDATE SET
                    data_type=excluded.data_type,
                    null_pct=excluded.null_pct,
                    distinct_count=excluded.distinct_count,
                    min_val=excluded.min_val,
                    max_val=excluded.max_val,
                    top_values_json=excluded.top_values_json,
                    inferred_domain=excluded.inferred_domain
            """, (
                table_id,
                col["name"],
                col.get("type"),
                col.get("null_pct"),
                col.get("distinct_count"),
                col.get("min"),
                col.get("max"),
                json.dumps(col.get("top_values", []), ensure_ascii=False),
                col.get("inferred_domain"),
            ))

        # datasource last_crawled_at 갱신
        conn.execute(
            "UPDATE datasource SET last_crawled_at=datetime('now','localtime') WHERE source_id=?",
            (source_id,)
        )

    return table_id


def get_profile(table_name: str, source_id: int) -> dict | None:
    """저장된 프로파일 반환. 없으면 None."""
    init_db()
    with _conn() as conn:
        row = conn.execute(
            "SELECT profile_json FROM table_meta WHERE source_id=? AND table_name=?",
            (source_id, table_name)
        ).fetchone()
    if row and row["profile_json"]:
        return json.loads(row["profile_json"])
    return None


def list_profiled_tables(source_id: int) -> list[dict]:
    """프로파일이 저장된 테이블 목록."""
    init_db()
    with _conn() as conn:
        rows = conn.execute("""
            SELECT table_name, row_count, crawled_at
            FROM table_meta WHERE source_id=? ORDER BY table_name
        """, (source_id,)).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Validation Rule CRUD
# ─────────────────────────────────────────────────────────────
def save_validation_rules(rules: list[dict]) -> list[int]:
    """
    검증 규칙 목록을 저장합니다.
    이미 동일한 (rule_name, source_table, target_table) 이 있으면 업데이트.
    Returns: 저장된 rule_id 목록
    """
    init_db()
    ids = []
    with _conn() as conn:
        for r in rules:
            cur = conn.execute("""
                INSERT INTO validation_rule
                    (rule_name, rule_type, tier, source_table, target_table, target_column,
                     rule_sql, severity, threshold, auto_generated, reason)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r.get("rule_name", ""),
                r.get("rule_type", ""),
                r.get("tier", 1),
                r.get("source_table"),
                r.get("target_table"),
                r.get("target_column"),
                r.get("sql") or r.get("rule_sql"),
                r.get("severity", "WARNING"),
                r.get("threshold"),
                1 if r.get("auto_generated", True) else 0,
                r.get("reason"),
            ))
            ids.append(cur.lastrowid)
    return ids


def list_validation_rules(
    target_table: str | None = None,
    active_only: bool = True,
) -> list[dict]:
    """검증 규칙 목록 조회."""
    init_db()
    sql = "SELECT * FROM validation_rule WHERE 1=1"
    params: list[Any] = []
    if active_only:
        sql += " AND is_active=1"
    if target_table:
        sql += " AND (target_table=? OR source_table=?)"
        params += [target_table, target_table]
    sql += " ORDER BY tier, rule_id"

    with _conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Validation Result 저장 / 이력 조회
# ─────────────────────────────────────────────────────────────
def new_execution_id() -> str:
    return str(uuid.uuid4())


def save_validation_run(results: list[dict], execution_id: str | None = None) -> str:
    """
    검증 실행 결과 목록을 저장합니다.
    Parameters:
        results: [
            {"rule_id": int|None, "rule_name": str, "status": str,
             "actual_value": str, "expected_value": str,
             "detail_json": dict|None, "ai_analysis": str|None}
        ]
        execution_id: 없으면 자동 생성
    Returns: execution_id
    """
    init_db()
    eid = execution_id or new_execution_id()
    with _conn() as conn:
        for r in results:
            conn.execute("""
                INSERT INTO validation_result
                    (execution_id, rule_id, rule_name, status,
                     actual_value, expected_value, detail_json, ai_analysis)
                VALUES (?,?,?,?,?,?,?,?)
            """, (
                eid,
                r.get("rule_id"),
                r.get("rule_name"),
                r.get("status", "ERROR"),
                r.get("actual_value"),
                r.get("expected_value"),
                json.dumps(r.get("detail_json") or {}, ensure_ascii=False),
                r.get("ai_analysis"),
            ))
    return eid


def get_validation_history(
    table_name: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """최근 검증 실행 이력을 반환합니다."""
    init_db()
    sql = """
        SELECT vr.*, r.rule_type, r.tier, r.severity
        FROM validation_result vr
        LEFT JOIN validation_rule r ON vr.rule_id = r.rule_id
        WHERE 1=1
    """
    params: list[Any] = []
    if table_name:
        sql += " AND (r.target_table=? OR r.source_table=?)"
        params += [table_name, table_name]
    sql += f" ORDER BY vr.run_timestamp DESC LIMIT {limit}"

    with _conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_execution_summary(execution_id: str) -> dict:
    """
    실행 ID별 요약 통계를 반환합니다.
    Returns: {"total": int, "pass": int, "fail": int, "warn": int, "error": int}
    """
    init_db()
    with _conn() as conn:
        rows = conn.execute("""
            SELECT status, COUNT(*) AS cnt
            FROM validation_result
            WHERE execution_id=?
            GROUP BY status
        """, (execution_id,)).fetchall()
    summary = {"total": 0, "pass": 0, "fail": 0, "warn": 0, "error": 0}
    for r in rows:
        key = r["status"].lower()
        cnt = r["cnt"]
        summary["total"] += cnt
        if key in summary:
            summary[key] += cnt
    return summary


# ─────────────────────────────────────────────────────────────
# CLI 확인용
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    print(f"DB 초기화 완료: {DB_PATH}")
    src_id = get_or_create_datasource("local_mariadb", "mariadb", "db_config.json")
    print(f"datasource id={src_id}")
    print("저장된 datasource 목록:", list_datasources())
