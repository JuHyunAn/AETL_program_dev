"""
================================================================================
AETL Data Profiler
================================================================================
DB에 직접 연결하여 테이블·컬럼 통계를 수집합니다.
수집된 프로파일은 AI 규칙 제안의 기반 데이터로 활용됩니다.

지원 DB: Oracle, MariaDB, PostgreSQL
================================================================================
"""

import re
from typing import Any

# ─────────────────────────────────────────────────────────────
# 도메인 추론 패턴
# ─────────────────────────────────────────────────────────────
_DOMAIN_PATTERNS = [
    ("email",   r".*e?mail.*"),
    ("phone",   r".*(phone|tel|mobile|hp|fax).*"),
    ("date",    r".*(date|dt|ymd|yyyymmdd).*"),
    ("amount",  r".*(amt|amount|price|cost|fee|sal|pay|revenue).*"),
    ("code",    r".*(cd|code|typ|type|stat|status|flag|yn|gb|div).*"),
    ("name",    r".*(name|nm|title).*"),
    ("id",      r".*(id|key|no|num|seq|idx).*"),
    ("address", r".*(addr|address|zip|post).*"),
    ("count",   r".*(cnt|count|qty|quantity).*"),
]

def _infer_domain(col_name: str, data_type: str) -> str:
    # 컬럼명 + 타입 패턴으로 도메인 추론.
    cn = col_name.lower()
    dt = data_type.lower()

    # 타입 우선 체크
    if any(t in dt for t in ("date", "timestamp", "datetime")):
        return "date"
    if any(t in dt for t in ("number", "numeric", "decimal", "float", "double", "int", "bigint")):
        for domain, pattern in _DOMAIN_PATTERNS:
            if re.match(pattern, cn, re.IGNORECASE):
                return domain
        return "numeric"
    if any(t in dt for t in ("char", "varchar", "text", "clob", "nchar", "nvarchar")):
        for domain, pattern in _DOMAIN_PATTERNS:
            if re.match(pattern, cn, re.IGNORECASE):
                return domain
        return "text"
    # 기타
    for domain, pattern in _DOMAIN_PATTERNS:
        if re.match(pattern, cn, re.IGNORECASE):
            return domain
    return "unknown"


# ─────────────────────────────────────────────────────────────
# SQL 빌더
# ─────────────────────────────────────────────────────────────
def _build_stats_sql_oracle(table_name: str, col_name: str) -> str:
    # Oracle용 컬럼 통계 SQL
    return f"""
SELECT
    COUNT(*)                              AS total_cnt,
    COUNT("{col_name}")                   AS non_null_cnt,
    COUNT(DISTINCT "{col_name}")          AS distinct_cnt,
    TO_CHAR(MIN("{col_name}"))            AS min_val,
    TO_CHAR(MAX("{col_name}"))            AS max_val
FROM "{table_name}"
"""

def _build_stats_sql_mariadb(table_name: str, col_name: str) -> str:
    # MariaDB용 컬럼 통계 SQL
    return f"""
SELECT
    COUNT(*)                AS total_cnt,
    COUNT(`{col_name}`)     AS non_null_cnt,
    COUNT(DISTINCT `{col_name}`) AS distinct_cnt,
    CAST(MIN(`{col_name}`) AS CHAR) AS min_val,
    CAST(MAX(`{col_name}`) AS CHAR) AS max_val
FROM `{table_name}`
"""

def _build_topval_sql_oracle(table_name: str, col_name: str, top_n: int = 10) -> str:
    return f"""
SELECT TO_CHAR("{col_name}") AS val, COUNT(*) AS cnt
FROM "{table_name}"
WHERE "{col_name}" IS NOT NULL
GROUP BY "{col_name}"
ORDER BY cnt DESC
FETCH FIRST {top_n} ROWS ONLY
"""

def _build_topval_sql_mariadb(table_name: str, col_name: str, top_n: int = 10) -> str:
    return f"""
SELECT CAST(`{col_name}` AS CHAR) AS val, COUNT(*) AS cnt
FROM `{table_name}`
WHERE `{col_name}` IS NOT NULL
GROUP BY `{col_name}`
ORDER BY cnt DESC
LIMIT {top_n}
"""

def _build_rowcount_sql_oracle(table_name: str) -> str:
    return f'SELECT COUNT(*) FROM "{table_name}"'

def _build_rowcount_sql_mariadb(table_name: str) -> str:
    return f"SELECT COUNT(*) FROM `{table_name}`"


def _build_stats_sql_postgresql(table_name: str, col_name: str) -> str:
    # PostgreSQL용 컬럼 통계 SQL
    return f"""
SELECT
    COUNT(*)                              AS total_cnt,
    COUNT("{col_name}")                   AS non_null_cnt,
    COUNT(DISTINCT "{col_name}")          AS distinct_cnt,
    CAST(MIN("{col_name}") AS TEXT)       AS min_val,
    CAST(MAX("{col_name}") AS TEXT)       AS max_val
FROM "{table_name}"
"""

def _build_topval_sql_postgresql(table_name: str, col_name: str, top_n: int = 10) -> str:
    # PostgreSQL용 상위 빈도값 SQL
    return f"""
SELECT CAST("{col_name}" AS TEXT) AS val, COUNT(*) AS cnt
FROM "{table_name}"
WHERE "{col_name}" IS NOT NULL
GROUP BY "{col_name}"
ORDER BY cnt DESC
LIMIT {top_n}
"""

def _build_rowcount_sql_postgresql(table_name: str) -> str:
    return f'SELECT COUNT(*) FROM "{table_name}"'


# ─────────────────────────────────────────────────────────────
# 컬럼 메타 조회 헬퍼
# ─────────────────────────────────────────────────────────────
def _get_column_info_oracle(cursor, table_name: str, owner: str | None) -> list[dict]:
    # Oracle 컬럼 타입 정보 조회
    if owner:
        cursor.execute("""
            SELECT column_name, data_type
            FROM all_tab_columns
            WHERE owner = :owner AND table_name = :tbl
            ORDER BY column_id
        """, {"owner": owner.upper(), "tbl": table_name})
    else:
        cursor.execute("""
            SELECT column_name, data_type
            FROM user_tab_columns
            WHERE table_name = :tbl
            ORDER BY column_id
        """, {"tbl": table_name})
    return [{"name": r[0], "type": r[1]} for r in cursor.fetchall()]


def _get_column_info_mariadb(cursor, db_name: str, table_name: str) -> list[dict]:
    # MariaDB 컬럼 타입 정보 조회
    cursor.execute("""
        SELECT COLUMN_NAME, COLUMN_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (db_name, table_name))
    return [{"name": r[0], "type": r[1]} for r in cursor.fetchall()]


def _get_column_info_postgresql(cursor, schema_name: str, table_name: str) -> list[dict]:
    # PostgreSQL 컬럼 타입 정보 조회
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema_name, table_name))
    return [{"name": r[0], "type": r[1]} for r in cursor.fetchall()]


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────
def profile_table(
    conn,
    table_name: str,
    db_type: str,
    owner: str | None = None,
    db_name: str | None = None,
    top_n: int = 10,
    skip_topval_types: tuple = ("clob", "blob", "text", "longtext"),
) -> dict:
    """
    테이블 컬럼별 통계를 수집하여 프로파일 딕셔너리를 반환합니다.

    Parameters:
        conn       : 활성 DB 커넥션 (oracledb / mariadb / psycopg2)
        table_name : 프로파일링할 테이블명
        db_type    : "oracle" | "mariadb" | "postgresql"
        owner      : Oracle owner (스키마명) / PostgreSQL schema명
        db_name    : MariaDB database명
        top_n      : 빈도 상위 N개 값 수집 수
        skip_topval_types : top_values 수집을 건너뛸 데이터 타입 키워드

    Returns:
        {
          "table_name": str,
          "row_count": int,
          "columns": [
            {
              "name": str,
              "type": str,
              "null_pct": float,       # 0.0 ~ 1.0
              "distinct_count": int,
              "min": str | None,
              "max": str | None,
              "top_values": [{"value": str, "count": int}, ...],
              "inferred_domain": str
            }
          ]
        }
    """
    db_type_lower = db_type.lower()
    is_oracle     = db_type_lower == "oracle"
    is_postgres   = db_type_lower in ("postgresql", "postgres")
    cursor        = conn.cursor()

    # PostgreSQL: "schema.table" → schema와 table 분리
    pg_schema = owner or "public"
    pg_table  = table_name
    if is_postgres and "." in table_name:
        pg_schema, pg_table = table_name.split(".", 1)

    # 실제 SQL에 사용할 테이블 참조명 (PostgreSQL은 schema.table 형태)
    sql_table_ref = f'"{pg_schema}"."{pg_table}"' if is_postgres else table_name

    # 1. 전체 건수
    if is_oracle:
        rc_sql = _build_rowcount_sql_oracle(table_name)
    elif is_postgres:
        rc_sql = f'SELECT COUNT(*) FROM {sql_table_ref}'
    else:
        rc_sql = _build_rowcount_sql_mariadb(table_name)
    cursor.execute(rc_sql)
    row_count = cursor.fetchone()[0] or 0

    # 2. 컬럼 목록 조회
    if is_oracle:
        col_infos = _get_column_info_oracle(cursor, table_name, owner)
    elif is_postgres:
        col_infos = _get_column_info_postgresql(cursor, pg_schema, pg_table)
    else:
        col_infos = _get_column_info_mariadb(cursor, db_name or "", table_name)

    columns = []
    for ci in col_infos:
        cname = ci["name"]
        ctype = ci["type"]

        # 3. 컬럼 통계
        try:
            if is_oracle:
                stats_sql = _build_stats_sql_oracle(table_name, cname)
            elif is_postgres:
                stats_sql = _build_stats_sql_postgresql(sql_table_ref, cname)
            else:
                stats_sql = _build_stats_sql_mariadb(table_name, cname)
            cursor.execute(stats_sql)
            row = cursor.fetchone()
            total_cnt    = int(row[0]) if row[0] else 0
            non_null_cnt = int(row[1]) if row[1] else 0
            distinct_cnt = int(row[2]) if row[2] else 0
            min_val      = str(row[3]) if row[3] is not None else None
            max_val      = str(row[4]) if row[4] is not None else None
            null_pct     = round(1.0 - non_null_cnt / total_cnt, 4) if total_cnt > 0 else 0.0
        except Exception:
            null_pct = distinct_cnt = 0
            min_val = max_val = None

        # 4. Top Values (LOB 계열 제외)
        top_values: list[dict[str, Any]] = []
        skip = any(t in ctype.lower() for t in skip_topval_types)
        if not skip and row_count > 0:
            try:
                if is_oracle:
                    tv_sql = _build_topval_sql_oracle(table_name, cname, top_n)
                elif is_postgres:
                    tv_sql = _build_topval_sql_postgresql(sql_table_ref, cname, top_n)
                else:
                    tv_sql = _build_topval_sql_mariadb(table_name, cname, top_n)
                cursor.execute(tv_sql)
                top_values = [{"value": str(r[0]), "count": int(r[1])} for r in cursor.fetchall()]
            except Exception:
                pass

        columns.append({
            "name":            cname,
            "type":            ctype,
            "null_pct":        null_pct,
            "distinct_count":  distinct_cnt,
            "min":             min_val,
            "max":             max_val,
            "top_values":      top_values,
            "inferred_domain": _infer_domain(cname, ctype),
        })

    cursor.close()

    return {
        "table_name": table_name,
        "row_count":  row_count,
        "columns":    columns,
    }


def profile_table_from_config(
    config_path: str = "db_config.json",
    table_name: str = "",
    top_n: int = 10,
) -> dict:
    """
    db_config.json 을 읽어 자동으로 DB 연결 후 프로파일링합니다.
    Streamlit/CLI에서 간단히 호출할 때 사용합니다.
    """
    import os
    from dotenv import load_dotenv
    load_dotenv()

    from db_schema import load_config
    config = load_config(config_path)
    db_type = config.get("db_type", "oracle").lower()
    conn_cfg = config["connection"]

    if db_type == "oracle":
        import oracledb
        dsn  = f"{conn_cfg['host']}:{conn_cfg['port']}/{conn_cfg['database']}"
        conn = oracledb.connect(user=conn_cfg["user"], password=conn_cfg["password"], dsn=dsn)
        owner = config.get("schema_options", {}).get("owner")
        result = profile_table(conn, table_name, "oracle", owner=owner, top_n=top_n)
        conn.close()

    elif db_type == "mariadb":
        import mariadb
        conn = mariadb.connect(
            host=conn_cfg["host"],
            port=int(conn_cfg.get("port", 3306)),
            user=conn_cfg["user"],
            password=conn_cfg["password"],
            database=conn_cfg["database"],
        )
        result = profile_table(conn, table_name, "mariadb",
                               db_name=conn_cfg["database"], top_n=top_n)
        conn.close()

    elif db_type in ("postgresql", "postgres"):
        import psycopg2
        conn = psycopg2.connect(
            host=conn_cfg["host"],
            port=int(conn_cfg.get("port", 5432)),
            user=conn_cfg["user"],
            password=conn_cfg["password"],
            dbname=conn_cfg["database"],
        )
        owner = config.get("schema_options", {}).get("owner")
        result = profile_table(conn, table_name, "postgresql",
                               owner=owner, top_n=top_n)
        conn.close()
    else:
        raise ValueError(f"지원하지 않는 db_type: {db_type}")

    return result


def profile_summary_text(profile: dict) -> str:
    """
    프로파일 결과를 LLM 프롬프트용 간결한 텍스트로 변환합니다.
    """
    lines = [f"[{profile['table_name']}] row_count={profile['row_count']:,}"]
    for c in profile["columns"]:
        null_str = f"null={c['null_pct']*100:.1f}%" if c["null_pct"] > 0 else "null=0%"
        domain   = c["inferred_domain"]
        lines.append(
            f"  {c['name']} ({c['type']}) | {null_str} | distinct={c['distinct_count']} "
            f"| domain={domain} | range=[{c['min']} ~ {c['max']}]"
        )
    return "\n".join(lines)
