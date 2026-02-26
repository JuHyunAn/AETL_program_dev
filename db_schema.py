"""
================================================================================
                    Oracle / MariaDB 스키마 조회 모듈
================================================================================
[기능]
- Oracle 또는 MariaDB에 연결하여 메타데이터 조회
- SCHEMA 딕셔너리 형태로 변환
- 캐싱 지원

[드라이버]
- Oracle: oracledb (Thin 모드, Oracle Client 불필요)
- MariaDB: mariadb

[작성자] 안주현
[최종 수정] 2026-01-20
================================================================================
"""

import json
import os
import re
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pathlib import Path


# =============================================================================
# 상수 정의
# =============================================================================
CONFIG_FILE = "db_config.json"
CACHE_FILE = ".schema_cache.json"


# =============================================================================
# 설정 파일 로드
# =============================================================================
def load_config(config_path: str = CONFIG_FILE) -> Dict[str, Any]:
    """
    설정 파일을 로드합니다.
    환경변수 참조(${VAR_NAME})를 실제 값으로 치환합니다.

    Parameters:
        config_path: 설정 파일 경로

    Returns:
        설정 딕셔너리
    """
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # 환경변수 치환 (${VAR_NAME} 형태)
    def replace_env_vars(obj):
        if isinstance(obj, str):
            if obj.startswith("${") and obj.endswith("}"):
                var_name = obj[2:-1]
                return os.environ.get(var_name, obj)
            return obj
        elif isinstance(obj, dict):
            return {k: replace_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [replace_env_vars(item) for item in obj]
        return obj

    return replace_env_vars(config)


# =============================================================================
# 추상 베이스 클래스
# =============================================================================
class SchemaFetcher(ABC):
    """
    스키마 조회를 위한 추상 베이스 클래스
    Oracle, MariaDB 등 각 DB별로 구현체를 제공
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None
        self.cursor = None
        self.schema_options = config.get("schema_options", {})
        self.owner = self.schema_options.get("owner")
        self.include_tables = self.schema_options.get("include_tables", [])
        self.exclude_tables = self.schema_options.get("exclude_tables", [])
        self.include_views = self.schema_options.get("include_views", False)

    @abstractmethod
    def connect(self):
        """DB에 연결합니다."""
        pass

    @abstractmethod
    def close(self):
        """DB 연결을 닫습니다."""
        pass

    @abstractmethod
    def get_tables(self) -> List[str]:
        """테이블 목록을 조회합니다."""
        pass

    @abstractmethod
    def get_columns(self, table_name: str) -> List[str]:
        """특정 테이블의 컬럼 목록을 조회합니다."""
        pass

    @abstractmethod
    def get_primary_keys(self, table_name: str) -> List[str]:
        """특정 테이블의 기본키 컬럼을 조회합니다."""
        pass

    @abstractmethod
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """특정 테이블의 외래키 정보를 조회합니다."""
        pass

    def filter_tables(self, tables: List[str]) -> List[str]:
        """include/exclude 옵션에 따라 테이블 필터링"""
        result = tables

        # include 필터링
        if self.include_tables:
            include_set = set(t.upper() for t in self.include_tables)
            result = [t for t in result if t.upper() in include_set]

        # exclude 필터링
        if self.exclude_tables:
            for pattern in self.exclude_tables:
                # SQL LIKE 패턴을 정규표현식으로 변환
                regex = "^" + pattern.replace("%", ".*").replace("_", ".") + "$"
                result = [t for t in result if not re.match(regex, t, re.IGNORECASE)]

        return result

    def build_joins_from_fk(self, tables: Dict[str, Any]) -> List[Dict[str, str]]:
        """외래키 정보를 기반으로 조인 규칙을 생성합니다."""
        joins = []
        seen = set()  # 중복 방지

        for table_name, info in tables.items():
            for fk in info.get("fk", []):
                left = f"{table_name}.{fk['col']}"
                right = f"{fk['ref_table']}.{fk['ref_col']}"

                # 중복 체크 (양방향)
                key = tuple(sorted([left, right]))
                if key not in seen:
                    joins.append({"left": left, "right": right})
                    seen.add(key)

        return joins

    def fetch_schema(self) -> Dict[str, Any]:
        """
        스키마 전체를 조회하여 딕셔너리로 반환합니다.

        Returns:
            {
                "tables": {...},
                "joins": [...],
                "synonyms": {...}
            }
        """
        self.connect()

        try:
            # 1. 테이블 목록 조회
            table_names = self.get_tables()
            table_names = self.filter_tables(table_names)

            # 2. 각 테이블의 상세 정보 조회
            tables = {}
            for table_name in table_names:
                columns = self.get_columns(table_name)
                pk = self.get_primary_keys(table_name)
                fk = self.get_foreign_keys(table_name)

                tables[table_name] = {
                    "columns": columns,
                    "pk": pk,
                    "fk": fk
                }

            # 3. 조인 규칙 생성 (외래키 기반)
            joins = self.build_joins_from_fk(tables)

            # 4. 동의어는 설정 파일에서 로드 (자동 추출 불가)
            synonyms = self.config.get("synonyms", {})

            return {
                "tables": tables,
                "joins": joins,
                "synonyms": synonyms
            }

        finally:
            self.close()


# =============================================================================
# Oracle 구현체
# =============================================================================
class OracleSchemaFetcher(SchemaFetcher):
    """Oracle DB용 스키마 조회 구현체"""

    def connect(self):
        try:
            import oracledb
        except ImportError:
            raise ImportError("oracledb 패키지가 설치되지 않았습니다. pip install oracledb")

        conn_config = self.config["connection"]

        # DSN 구성: host:port/service_name
        dsn = f"{conn_config['host']}:{conn_config['port']}/{conn_config['database']}"

        self.conn = oracledb.connect(
            user=conn_config["user"],
            password=conn_config["password"],
            dsn=dsn
        )
        self.cursor = self.conn.cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_tables(self) -> List[str]:
        if self.owner:
            sql = """
                SELECT table_name
                FROM all_tables
                WHERE owner = :owner
                ORDER BY table_name
            """
            self.cursor.execute(sql, {"owner": self.owner.upper()})
        else:
            sql = """
                SELECT table_name
                FROM user_tables
                ORDER BY table_name
            """
            self.cursor.execute(sql)

        tables = [row[0] for row in self.cursor.fetchall()]

        # 뷰 포함 시
        if self.include_views:
            if self.owner:
                sql = "SELECT view_name FROM all_views WHERE owner = :owner ORDER BY view_name"
                self.cursor.execute(sql, {"owner": self.owner.upper()})
            else:
                sql = "SELECT view_name FROM user_views ORDER BY view_name"
                self.cursor.execute(sql)
            tables.extend([row[0] for row in self.cursor.fetchall()])

        return tables

    def get_columns(self, table_name: str) -> List[str]:
        if self.owner:
            sql = """
                SELECT column_name
                FROM all_tab_columns
                WHERE owner = :owner AND table_name = :table_name
                ORDER BY column_id
            """
            self.cursor.execute(sql, {"owner": self.owner.upper(), "table_name": table_name})
        else:
            sql = """
                SELECT column_name
                FROM user_tab_columns
                WHERE table_name = :table_name
                ORDER BY column_id
            """
            self.cursor.execute(sql, {"table_name": table_name})

        return [row[0] for row in self.cursor.fetchall()]

    def get_primary_keys(self, table_name: str) -> List[str]:
        if self.owner:
            sql = """
                SELECT cc.column_name
                FROM all_constraints c
                JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name
                                         AND c.owner = cc.owner
                WHERE c.owner = :owner
                  AND c.table_name = :table_name
                  AND c.constraint_type = 'P'
                ORDER BY cc.position
            """
            self.cursor.execute(sql, {"owner": self.owner.upper(), "table_name": table_name})
        else:
            sql = """
                SELECT cc.column_name
                FROM user_constraints c
                JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
                WHERE c.table_name = :table_name
                  AND c.constraint_type = 'P'
                ORDER BY cc.position
            """
            self.cursor.execute(sql, {"table_name": table_name})

        return [row[0] for row in self.cursor.fetchall()]

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        if self.owner:
            sql = """
                SELECT
                    cc.column_name AS fk_column,
                    rc.table_name AS ref_table,
                    rcc.column_name AS ref_column
                FROM all_constraints c
                JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name
                                         AND c.owner = cc.owner
                JOIN all_constraints rc ON c.r_constraint_name = rc.constraint_name
                                        AND c.r_owner = rc.owner
                JOIN all_cons_columns rcc ON rc.constraint_name = rcc.constraint_name
                                          AND rc.owner = rcc.owner
                                          AND cc.position = rcc.position
                WHERE c.owner = :owner
                  AND c.table_name = :table_name
                  AND c.constraint_type = 'R'
                ORDER BY cc.position
            """
            self.cursor.execute(sql, {"owner": self.owner.upper(), "table_name": table_name})
        else:
            sql = """
                SELECT
                    cc.column_name AS fk_column,
                    rc.table_name AS ref_table,
                    rcc.column_name AS ref_column
                FROM user_constraints c
                JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
                JOIN user_constraints rc ON c.r_constraint_name = rc.constraint_name
                JOIN user_cons_columns rcc ON rc.constraint_name = rcc.constraint_name
                                           AND cc.position = rcc.position
                WHERE c.table_name = :table_name
                  AND c.constraint_type = 'R'
                ORDER BY cc.position
            """
            self.cursor.execute(sql, {"table_name": table_name})

        return [
            {"col": row[0], "ref_table": row[1], "ref_col": row[2]}
            for row in self.cursor.fetchall()
        ]


# =============================================================================
# MariaDB 구현체
# =============================================================================
class MariaDBSchemaFetcher(SchemaFetcher):
    """MariaDB용 스키마 조회 구현체"""

    def connect(self):
        try:
            import mariadb
        except ImportError:
            raise ImportError("mariadb 패키지가 설치되지 않았습니다. pip install mariadb")

        conn_config = self.config["connection"]

        self.conn = mariadb.connect(
            host=conn_config["host"],
            port=conn_config["port"],
            user=conn_config["user"],
            password=conn_config["password"],
            database=conn_config["database"]
        )
        self.cursor = self.conn.cursor()

        # 사용할 데이터베이스 (스키마)
        self.database = self.owner if self.owner else conn_config["database"]

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_tables(self) -> List[str]:
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        self.cursor.execute(sql, (self.database,))
        tables = [row[0] for row in self.cursor.fetchall()]

        # 뷰 포함 시
        if self.include_views:
            sql = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'VIEW'
                ORDER BY table_name
            """
            self.cursor.execute(sql, (self.database,))
            tables.extend([row[0] for row in self.cursor.fetchall()])

        return tables

    def get_columns(self, table_name: str) -> List[str]:
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        self.cursor.execute(sql, (self.database, table_name))
        return [row[0] for row in self.cursor.fetchall()]

    def get_primary_keys(self, table_name: str) -> List[str]:
        sql = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """
        self.cursor.execute(sql, (self.database, table_name))
        return [row[0] for row in self.cursor.fetchall()]

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        sql = """
            SELECT
                kcu.column_name AS fk_column,
                kcu.referenced_table_name AS ref_table,
                kcu.referenced_column_name AS ref_column
            FROM information_schema.key_column_usage kcu
            WHERE kcu.table_schema = %s
              AND kcu.table_name = %s
              AND kcu.referenced_table_name IS NOT NULL
            ORDER BY kcu.ordinal_position
        """
        self.cursor.execute(sql, (self.database, table_name))
        return [
            {"col": row[0], "ref_table": row[1], "ref_col": row[2]}
            for row in self.cursor.fetchall()
        ]


# =============================================================================
# 팩토리 함수
# =============================================================================
def get_fetcher(config: Dict[str, Any]) -> SchemaFetcher:
    """
    DB 타입에 따라 적절한 SchemaFetcher 인스턴스를 반환합니다.

    Parameters:
        config: 설정 딕셔너리

    Returns:
        SchemaFetcher 구현체 인스턴스
    """
    db_type = config.get("db_type", "oracle").lower()

    if db_type == "oracle":
        return OracleSchemaFetcher(config)
    elif db_type in ("mariadb", "mysql"):
        return MariaDBSchemaFetcher(config)
    else:
        raise ValueError(f"지원하지 않는 DB 타입입니다: {db_type}")


# =============================================================================
# 캐싱 지원
# =============================================================================
def get_cache_path(config_path: str) -> str:
    """설정 파일과 같은 디렉토리에 캐시 파일 경로 반환"""
    config_dir = os.path.dirname(os.path.abspath(config_path))
    return os.path.join(config_dir, CACHE_FILE)


def load_cached_schema(cache_file: str, ttl: int = 3600) -> Optional[Dict[str, Any]]:
    """
    캐시 파일에서 스키마를 로드합니다.
    TTL이 지났으면 None을 반환합니다.

    Parameters:
        cache_file: 캐시 파일 경로
        ttl: 캐시 유효 시간 (초)

    Returns:
        스키마 딕셔너리 또는 None
    """
    if not os.path.exists(cache_file):
        return None

    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)

        cached_time = cached.get("_cached_at", 0)
        if time.time() - cached_time > ttl:
            return None

        # 메타데이터 제거 후 반환
        schema = {k: v for k, v in cached.items() if not k.startswith("_")}
        return schema

    except (json.JSONDecodeError, KeyError):
        return None


def save_schema_to_cache(schema: Dict[str, Any], cache_file: str):
    """
    스키마를 캐시 파일에 저장합니다.

    Parameters:
        schema: 저장할 스키마 딕셔너리
        cache_file: 캐시 파일 경로
    """
    cached = schema.copy()
    cached["_cached_at"] = time.time()
    cached["_db_type"] = schema.get("_db_type", "unknown")

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cached, f, ensure_ascii=False, indent=2)


# =============================================================================
# 외부 호출용 메인 함수
# =============================================================================
def get_schema(
    config_path: str = CONFIG_FILE,
    force_refresh: bool = False
) -> Dict[str, Any]:
    """
    스키마를 반환합니다.
    캐싱이 활성화되어 있으면 캐시를 우선 사용합니다.

    Parameters:
        config_path: 설정 파일 경로
        force_refresh: True면 캐시를 무시하고 DB에서 새로 조회

    Returns:
        SCHEMA 딕셔너리:
        {
            "tables": {...},
            "joins": [...],
            "synonyms": {...},
            "_db_type": "oracle" | "mariadb"
        }
    """
    config = load_config(config_path)
    cache_enabled = config.get("cache", {}).get("enabled", True)
    cache_ttl = config.get("cache", {}).get("ttl_seconds", 3600)
    cache_file = get_cache_path(config_path)

    # 캐시 확인
    if cache_enabled and not force_refresh:
        cached = load_cached_schema(cache_file, cache_ttl)
        if cached:
            print(f"[INFO] 캐시에서 스키마 로드됨 ({cache_file})")
            return cached

    # DB에서 조회
    db_type = config.get("db_type", "oracle")
    print(f"[INFO] {db_type.upper()} DB에서 스키마 조회 중...")

    fetcher = get_fetcher(config)
    schema = fetcher.fetch_schema()

    # DB 타입 정보 추가
    schema["_db_type"] = db_type.lower()

    # 캐시 저장
    if cache_enabled:
        save_schema_to_cache(schema, cache_file)
        print(f"[INFO] 스키마 캐시 저장됨 ({cache_file})")

    return schema


def get_db_type(config_path: str = CONFIG_FILE) -> str:
    """
    설정 파일에서 DB 타입을 반환합니다.

    Parameters:
        config_path: 설정 파일 경로

    Returns:
        "oracle" 또는 "mariadb"
    """
    config = load_config(config_path)
    return config.get("db_type", "oracle").lower()


# =============================================================================
# CLI 테스트
# =============================================================================
if __name__ == "__main__":
    import sys

    config_path = CONFIG_FILE
    force = False

    # 인자 파싱
    for arg in sys.argv[1:]:
        if arg == "--refresh":
            force = True
        elif arg.startswith("--config="):
            config_path = arg.split("=")[1]

    try:
        schema = get_schema(config_path, force_refresh=force)

        print(f"\n{'='*60}")
        print(f"DB 타입: {schema.get('_db_type', 'unknown').upper()}")
        print(f"테이블 수: {len(schema['tables'])}")
        print(f"조인 규칙 수: {len(schema['joins'])}")
        print(f"동의어 수: {len(schema['synonyms'])}")
        print(f"{'='*60}")

        print("\n[테이블 목록]")
        for table_name, info in schema["tables"].items():
            pk_str = ", ".join(info["pk"]) if info["pk"] else "(없음)"
            fk_count = len(info["fk"])
            print(f"  - {table_name}: {len(info['columns'])} columns, PK: {pk_str}, FK: {fk_count}개")

        if schema["joins"]:
            print("\n[조인 규칙]")
            for join in schema["joins"]:
                print(f"  - {join['left']} = {join['right']}")

    except FileNotFoundError:
        print(f"[ERROR] 설정 파일을 찾을 수 없습니다: {config_path}")
        print("db_config.json 파일을 생성하고 연결 정보를 입력하세요.")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] 스키마 조회 실패: {e}")
        sys.exit(1)
