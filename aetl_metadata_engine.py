"""
================================================================================
AETL Metadata Engine
================================================================================
DB 스키마·프로파일을 SQLite에 사전 수집하여 Agent가 라이브 DB 없이 빠르게 조회할 수 있게 합니다.

저장소: .aetl_metadata.db (SQLite)

스키마:
  meta_tables  — 테이블 목록 + 행 수
  meta_columns — 컬럼 메타데이터 (타입, PK, FK)
  meta_profiles — 컬럼별 통계 (null 비율, distinct 수, min/max, top 값)

Public API:
  sync_schema()             — db_schema → SQLite 동기화
  sync_profile()            — aetl_profiler → SQLite 동기화 (느림)
  get_all_tables()          — 테이블 목록 조회
  get_table_schema_from_meta(table_name) — 스키마 조회
  search_tables_from_meta(keyword)       — 키워드 검색
  get_profile_from_meta(table_name)      — 프로파일 조회
  is_schema_synced()        — 메타데이터 존재 여부
  clear_metadata()          — 전체 초기화
================================================================================
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any

_DB_FILE = ".aetl_metadata.db"
_PROFILE_TTL_HOURS = 24  # 프로파일은 24시간 이내면 재수집 생략


# ─────────────────────────────────────────────────────────────
# DB 경로 및 초기화
# ─────────────────────────────────────────────────────────────

def get_db_path() -> str:
    """메타데이터 SQLite DB 파일 경로 반환."""
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, _DB_FILE)


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    """테이블이 없으면 생성."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meta_tables (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT    UNIQUE NOT NULL,
            db_type    TEXT,
            row_count  INTEGER DEFAULT 0,
            synced_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS meta_columns (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT    NOT NULL,
            col_name   TEXT    NOT NULL,
            data_type  TEXT,
            is_pk      INTEGER DEFAULT 0,
            fk_ref     TEXT,
            UNIQUE(table_name, col_name)
        );

        CREATE TABLE IF NOT EXISTS meta_profiles (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name    TEXT    NOT NULL,
            col_name      TEXT    NOT NULL,
            total_cnt     INTEGER DEFAULT 0,
            null_ratio    REAL    DEFAULT 0.0,
            distinct_cnt  INTEGER DEFAULT 0,
            min_val       TEXT,
            max_val       TEXT,
            top_vals      TEXT,
            inferred_domain TEXT,
            synced_at     TEXT,
            UNIQUE(table_name, col_name)
        );
    """)
    conn.commit()


# ─────────────────────────────────────────────────────────────
# 스키마 동기화
# ─────────────────────────────────────────────────────────────

def sync_schema(
    config_path: str = "db_config.json",
    tables: list[str] | None = None,
) -> dict:
    """
    db_schema.get_schema()로 스키마를 읽어 SQLite에 저장합니다.

    Args:
        config_path : db_config.json 경로
        tables      : None → 전체, list → 지정 테이블만

    Returns:
        {"synced": [...], "skipped": [], "error": [...]}
    """
    result: dict[str, list] = {"synced": [], "skipped": [], "error": []}

    try:
        from db_schema import get_schema, get_db_type
        schema = get_schema(config_path, force_refresh=False)
        db_type = get_db_type(config_path)
    except Exception as e:
        result["error"].append(f"스키마 로딩 실패: {e}")
        return result

    all_tables: dict[str, Any] = schema.get("tables", {})
    target_keys = [
        k for k in all_tables
        if tables is None or k.upper() in [t.upper() for t in tables]
    ]

    now_iso = datetime.now().isoformat(timespec="seconds")

    conn = _get_conn()
    _init_db(conn)

    try:
        for tbl_name in target_keys:
            try:
                info = all_tables[tbl_name]
                pk_cols = [c.upper() for c in info.get("pk", [])]
                fk_map: dict[str, str] = {}
                for fk in info.get("fk", []):
                    ref = f"{fk.get('ref_table','')}.{fk.get('ref_col','')}"
                    fk_map[fk.get("col", "").upper()] = ref

                # meta_tables upsert
                conn.execute("""
                    INSERT INTO meta_tables (table_name, db_type, row_count, synced_at)
                    VALUES (?, ?, 0, ?)
                    ON CONFLICT(table_name) DO UPDATE SET
                        db_type   = excluded.db_type,
                        synced_at = excluded.synced_at
                """, (tbl_name, db_type, now_iso))

                # meta_columns upsert
                columns = info.get("columns", [])
                for col in columns:
                    col_name = col if isinstance(col, str) else col.get("name", "")
                    col_type = "" if isinstance(col, str) else col.get("type", "")
                    is_pk = 1 if col_name.upper() in pk_cols else 0
                    fk_ref = fk_map.get(col_name.upper())
                    conn.execute("""
                        INSERT INTO meta_columns (table_name, col_name, data_type, is_pk, fk_ref)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(table_name, col_name) DO UPDATE SET
                            data_type = excluded.data_type,
                            is_pk     = excluded.is_pk,
                            fk_ref    = excluded.fk_ref
                    """, (tbl_name, col_name, col_type, is_pk, fk_ref))

                result["synced"].append(tbl_name)
            except Exception as e:
                result["error"].append(f"{tbl_name}: {e}")

        conn.commit()
    finally:
        conn.close()

    return result


# ─────────────────────────────────────────────────────────────
# 프로파일 동기화
# ─────────────────────────────────────────────────────────────

def sync_profile(
    config_path: str = "db_config.json",
    tables: list[str] | None = None,
    force: bool = False,
) -> dict:
    """
    aetl_profiler를 사용해 테이블 프로파일을 수집 후 SQLite에 저장합니다.

    Args:
        config_path : db_config.json 경로
        tables      : None → meta_tables에 있는 전체, list → 지정 테이블만
        force       : True → TTL 무시하고 강제 재수집

    Returns:
        {"synced": [...], "skipped": [...], "error": [...]}
    """
    result: dict[str, list] = {"synced": [], "skipped": [], "error": []}

    conn = _get_conn()
    _init_db(conn)

    # 대상 테이블 결정
    if tables:
        target_tables = [t.upper() for t in tables]
    else:
        rows = conn.execute("SELECT table_name FROM meta_tables").fetchall()
        if not rows:
            # 스키마 동기화 먼저 필요
            conn.close()
            schema_result = sync_schema(config_path)
            if schema_result["error"] and not schema_result["synced"]:
                result["error"].append("스키마 동기화 실패 — 프로파일 수집 불가")
                return result
            conn = _get_conn()
        rows = conn.execute("SELECT table_name FROM meta_tables").fetchall()
        target_tables = [r["table_name"] for r in rows]

    cutoff = (datetime.now() - timedelta(hours=_PROFILE_TTL_HOURS)).isoformat(timespec="seconds")
    now_iso = datetime.now().isoformat(timespec="seconds")

    try:
        from aetl_profiler import profile_table_from_config
    except ImportError as e:
        conn.close()
        result["error"].append(f"aetl_profiler 임포트 실패: {e}")
        return result

    for tbl_name in target_tables:
        try:
            # TTL 체크 (force=False일 때)
            if not force:
                row = conn.execute("""
                    SELECT MIN(synced_at) AS oldest
                    FROM meta_profiles WHERE table_name = ?
                """, (tbl_name,)).fetchone()
                oldest = row["oldest"] if row else None
                if oldest and oldest > cutoff:
                    result["skipped"].append(tbl_name)
                    continue

            # 라이브 프로파일링
            profile = profile_table_from_config(config_path, tbl_name, top_n=10)

            for col in profile["columns"]:
                top_json = json.dumps(col.get("top_values", []), ensure_ascii=False)
                conn.execute("""
                    INSERT INTO meta_profiles
                        (table_name, col_name, total_cnt, null_ratio, distinct_cnt,
                         min_val, max_val, top_vals, inferred_domain, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(table_name, col_name) DO UPDATE SET
                        total_cnt       = excluded.total_cnt,
                        null_ratio      = excluded.null_ratio,
                        distinct_cnt    = excluded.distinct_cnt,
                        min_val         = excluded.min_val,
                        max_val         = excluded.max_val,
                        top_vals        = excluded.top_vals,
                        inferred_domain = excluded.inferred_domain,
                        synced_at       = excluded.synced_at
                """, (
                    tbl_name, col["name"],
                    profile["row_count"],
                    col["null_pct"],
                    col["distinct_count"],
                    col.get("min"), col.get("max"),
                    top_json,
                    col.get("inferred_domain", "unknown"),
                    now_iso,
                ))

            # meta_tables row_count 업데이트
            conn.execute("""
                UPDATE meta_tables SET row_count = ? WHERE table_name = ?
            """, (profile["row_count"], tbl_name))

            conn.commit()
            result["synced"].append(tbl_name)

        except Exception as e:
            result["error"].append(f"{tbl_name}: {e}")

    conn.close()
    return result


# ─────────────────────────────────────────────────────────────
# 조회 API
# ─────────────────────────────────────────────────────────────

def is_schema_synced() -> bool:
    """meta_tables에 1건 이상 있으면 True."""
    try:
        conn = _get_conn()
        _init_db(conn)
        cnt = conn.execute("SELECT COUNT(*) FROM meta_tables").fetchone()[0]
        conn.close()
        return cnt > 0
    except Exception:
        return False


def get_all_tables() -> list[dict]:
    """
    SQLite에서 테이블 목록을 반환합니다.

    Returns:
        [{"table_name": str, "db_type": str, "row_count": int, "synced_at": str}, ...]
    """
    try:
        conn = _get_conn()
        _init_db(conn)
        rows = conn.execute(
            "SELECT table_name, db_type, row_count, synced_at FROM meta_tables ORDER BY table_name"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_table_schema_from_meta(table_name: str) -> dict | None:
    """
    SQLite에서 테이블 스키마(컬럼+PK+FK)를 반환합니다.
    없으면 None.

    Returns:
        {
          "table_name": str,
          "columns": [{"name": str, "type": str}],
          "pk": [str, ...],
          "fk": [{"col": str, "ref_table": str, "ref_col": str}]
        }
    """
    try:
        conn = _get_conn()
        _init_db(conn)

        tbl_upper = table_name.upper()
        # 대소문자 무관 검색
        row = conn.execute(
            "SELECT table_name FROM meta_tables WHERE UPPER(table_name) = ?", (tbl_upper,)
        ).fetchone()
        if not row:
            conn.close()
            return None

        matched_name = row["table_name"]
        col_rows = conn.execute(
            "SELECT col_name, data_type, is_pk, fk_ref FROM meta_columns WHERE table_name = ? ORDER BY id",
            (matched_name,),
        ).fetchall()
        conn.close()

        columns = []
        pk_cols = []
        fk_list = []
        for cr in col_rows:
            columns.append({"name": cr["col_name"], "type": cr["data_type"] or ""})
            if cr["is_pk"]:
                pk_cols.append(cr["col_name"])
            if cr["fk_ref"]:
                parts = cr["fk_ref"].split(".", 1)
                fk_list.append({
                    "col": cr["col_name"],
                    "ref_table": parts[0] if len(parts) > 1 else cr["fk_ref"],
                    "ref_col":   parts[1] if len(parts) > 1 else "",
                })

        return {
            "table_name": matched_name,
            "columns":    columns,
            "pk":         pk_cols,
            "fk":         fk_list,
        }
    except Exception:
        return None


def search_tables_from_meta(keyword: str) -> list[str]:
    """
    SQLite에서 테이블명에 keyword가 포함된 목록을 반환합니다.
    없으면 빈 리스트.
    """
    try:
        conn = _get_conn()
        _init_db(conn)
        kw = f"%{keyword.upper()}%"
        rows = conn.execute(
            "SELECT table_name FROM meta_tables WHERE UPPER(table_name) LIKE ? ORDER BY table_name",
            (kw,),
        ).fetchall()
        conn.close()
        return [r["table_name"] for r in rows]
    except Exception:
        return []


def get_profile_from_meta(table_name: str) -> dict | None:
    """
    SQLite에서 테이블 프로파일을 반환합니다.
    `profile_summary_text()`와 호환되는 딕셔너리 구조를 반환합니다.
    없으면 None.

    Returns:
        {
          "table_name": str,
          "row_count": int,
          "columns": [
            {
              "name": str, "type": str,
              "null_pct": float, "distinct_count": int,
              "min": str|None, "max": str|None,
              "top_values": [...],
              "inferred_domain": str
            }
          ]
        }
    """
    try:
        conn = _get_conn()
        _init_db(conn)

        tbl_upper = table_name.upper()
        tbl_row = conn.execute(
            "SELECT table_name, row_count FROM meta_tables WHERE UPPER(table_name) = ?",
            (tbl_upper,),
        ).fetchone()
        if not tbl_row:
            conn.close()
            return None

        matched_name = tbl_row["table_name"]
        row_count = tbl_row["row_count"] or 0

        # 프로파일 존재 여부 확인
        cnt = conn.execute(
            "SELECT COUNT(*) FROM meta_profiles WHERE table_name = ?", (matched_name,)
        ).fetchone()[0]
        if cnt == 0:
            conn.close()
            return None

        # 컬럼 타입은 meta_columns에서
        col_type_map: dict[str, str] = {}
        for cr in conn.execute(
            "SELECT col_name, data_type FROM meta_columns WHERE table_name = ?", (matched_name,)
        ).fetchall():
            col_type_map[cr["col_name"]] = cr["data_type"] or ""

        prof_rows = conn.execute(
            """SELECT col_name, total_cnt, null_ratio, distinct_cnt,
                      min_val, max_val, top_vals, inferred_domain
               FROM meta_profiles WHERE table_name = ? ORDER BY id""",
            (matched_name,),
        ).fetchall()
        conn.close()

        # row_count를 프로파일의 total_cnt에서 갱신 (프로파일이 더 최신일 수 있음)
        if prof_rows:
            row_count = prof_rows[0]["total_cnt"] or row_count

        columns = []
        for pr in prof_rows:
            top_vals = []
            try:
                top_vals = json.loads(pr["top_vals"] or "[]")
            except Exception:
                pass
            columns.append({
                "name":            pr["col_name"],
                "type":            col_type_map.get(pr["col_name"], ""),
                "null_pct":        float(pr["null_ratio"] or 0.0),
                "distinct_count":  int(pr["distinct_cnt"] or 0),
                "min":             pr["min_val"],
                "max":             pr["max_val"],
                "top_values":      top_vals,
                "inferred_domain": pr["inferred_domain"] or "unknown",
            })

        return {
            "table_name": matched_name,
            "row_count":  row_count,
            "columns":    columns,
        }
    except Exception:
        return None


def get_sync_status() -> dict:
    """
    동기화 상태 요약을 반환합니다.

    Returns:
        {
          "table_count": int,
          "profile_count": int,   # 프로파일이 수집된 테이블 수
          "last_schema_sync": str | None,
          "last_profile_sync": str | None,
        }
    """
    try:
        conn = _get_conn()
        _init_db(conn)

        tbl_cnt = conn.execute("SELECT COUNT(*) FROM meta_tables").fetchone()[0]
        # 프로파일이 있는 테이블 수
        prof_tbl_cnt = conn.execute(
            "SELECT COUNT(DISTINCT table_name) FROM meta_profiles"
        ).fetchone()[0]
        last_schema = conn.execute(
            "SELECT MAX(synced_at) FROM meta_tables"
        ).fetchone()[0]
        last_profile = conn.execute(
            "SELECT MAX(synced_at) FROM meta_profiles"
        ).fetchone()[0]
        conn.close()

        return {
            "table_count":       tbl_cnt,
            "profile_count":     prof_tbl_cnt,
            "last_schema_sync":  last_schema,
            "last_profile_sync": last_profile,
        }
    except Exception:
        return {"table_count": 0, "profile_count": 0,
                "last_schema_sync": None, "last_profile_sync": None}


def clear_metadata() -> bool:
    """메타데이터 전체 초기화."""
    try:
        conn = _get_conn()
        conn.executescript("""
            DELETE FROM meta_tables;
            DELETE FROM meta_columns;
            DELETE FROM meta_profiles;
        """)
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False
