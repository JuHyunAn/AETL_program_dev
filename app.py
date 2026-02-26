"""
================================================================================
                        Oracle SQL 생성 챗봇 - 핵심 엔진
                           (토큰 최적화 버전 v2.0)
================================================================================

이 파일은 자연어를 Oracle SQL로 변환하는 챗봇의 핵심 로직을 담당

[주요 기능]
- 사용자의 한국어 질문을 분석하여 SQL 쿼리로 변환
- Google Gemini LLM을 활용한 자연어 처리
- LangGraph를 이용한 단계별 처리 파이프라인
- 보안 정책 적용 (PII 보호, DML 금지 등)

[v2.0 토큰 최적화 변경사항]
================================================================================
- parse_intent + build_plan + generate_sql을 하나의 노드로 통합
- 한 번의 사용자 질문에 LLM 1~2회만 호출** (기존 3~4회 → 1~2회)
- 예상 토큰 절감: 약 50~60%
================================================================================

[처리 흐름 (v2.0)]
1. clarify_node: 질문이 모호한지 확인 (LLM 호출 없음)
2. generate_sql_node: 의도 파싱 + 계획 수립 + SQL 생성 (LLM 호출 1회로 통합)
3. validate_sql_node: 보안 및 문법 검증 (LLM 호출 없음)
4. repair_node: 검증 실패 시 재시도 (LLM 호출 1회 추가)

[작성자] 안주현
[최종 수정] 2026-01-20
[버전] 2.0 (토큰 최적화)
================================================================================
"""

# ============================================================================
# 라이브러리 임포트
# ============================================================================
import json          # JSON 데이터 처리용
import re            # 정규표현식 (문자열 패턴 매칭)
import time          # 실행 시간 측정용

# .env 파일에서 환경변수(API KEY 등) 불러오기
# .env 파일에 GOOGLE_API_KEY=xxx 형태로 저장해두면 자동으로 읽어옴
from dotenv import load_dotenv
load_dotenv()

# 타입 힌트용 (코드 가독성 향상, 실행에는 영향 없음)
from typing import TypedDict, List, Dict, Any, Set, Optional

# LangChain: LLM(대규모 언어모델)과 통신하기 위한 프레임워크
# ChatGoogleGenerativeAI: Google Gemini 모델을 사용하기 위한 클래스
from langchain_google_genai import ChatGoogleGenerativeAI

# LangGraph: 상태 기반 워크플로우를 구성하기 위한 라이브러리
# StateGraph: 노드와 엣지로 구성된 상태 그래프
# END: 그래프의 종료 지점을 나타내는 상수
from langgraph.graph import StateGraph, END


# ============================================================================
# 섹션 1: 데이터베이스 스키마 정의 (동적 로딩 + 폴백)
# ============================================================================
"""
SCHEMA 딕셔너리는 데이터베이스의 구조를 정의

[v2.1 동적 스키마 로딩]
- db_config.json이 있으면 Oracle/MariaDB에서 동적으로 스키마 조회
- 설정 파일이 없거나 연결 실패 시 DEFAULT_SCHEMA 사용 (폴백)

포함 내용:
- tables: 테이블 정보 (컬럼, 기본키, 외래키)
- joins: 테이블 간 조인 관계
- synonyms: 한국어 용어와 실제 테이블/컬럼 매핑
- _db_type: DB 타입 (oracle/mariadb)
"""
import os as _os

# 기본 스키마 (폴백용) - DB 연결 실패 시 사용
DEFAULT_SCHEMA = {
    # -----------------------------------------------------------------------
    # 테이블 정의
    # -----------------------------------------------------------------------
    "tables": {
        # 부서 테이블
        "DEPARTMENT": {
            "columns": ["DEPT_ID", "DEPT_NAME", "CREATED_AT"],
            "pk": ["DEPT_ID"],  # 기본키: 부서 ID
            "fk": [],
        },
        # 사원 테이블
        "EMPLOYEE": {
            "columns": [
                "EMP_ID",         # 사원 ID (기본키)
                "EMP_NAME",       # 사원 이름
                "DEPT_ID",        # 소속 부서 ID (외래키)
                "BIRTH_DATE",     # 생년월일
                "GENDER_CD",      # 성별 코드
                "PHONE_NO",       # 전화번호
                "HIRE_DATE",      # 입사일
                "SALARY_ANNUAL",  # 연봉
                "RRN_FRONT",      # 주민번호 앞자리 (공개 가능)
                "RRN_BACK",       # 주민번호 뒷자리 (⚠️ 민감정보 - 직접 조회 금지)
            ],
            "pk": ["EMP_ID"],
            "fk": [{"col": "DEPT_ID", "ref_table": "DEPARTMENT", "ref_col": "DEPT_ID"}],
        },
        # 부서별 일일 매출 테이블
        "DEPT_SALES_DAILY": {
            "columns": ["SALES_DATE", "DEPT_ID", "REVENUE_AMT"],
            "pk": ["SALES_DATE", "DEPT_ID"],  # 복합 기본키: 날짜 + 부서
            "fk": [{"col": "DEPT_ID", "ref_table": "DEPARTMENT", "ref_col": "DEPT_ID"}],
        },
    },

    # -----------------------------------------------------------------------
    # 조인 규칙 정의
    # LLM이 SQL을 생성할 때 어떤 테이블을 어떻게 연결할지 참고
    # -----------------------------------------------------------------------
    "joins": [
        {"left": "EMPLOYEE.DEPT_ID", "right": "DEPARTMENT.DEPT_ID"},
        {"left": "DEPT_SALES_DAILY.DEPT_ID", "right": "DEPARTMENT.DEPT_ID"},
    ],

    # -----------------------------------------------------------------------
    # 동의어(Synonyms) 정의
    # 사용자가 한국어로 질문했을 때 실제 테이블/컬럼으로 매핑
    # 예: "매출" → DEPT_SALES_DAILY.REVENUE_AMT
    # -----------------------------------------------------------------------
    "synonyms": {
        "부서": "DEPARTMENT",
        "사원": "EMPLOYEE",
        "매출": "DEPT_SALES_DAILY.REVENUE_AMT",
        "실적": "DEPT_SALES_DAILY.REVENUE_AMT",
        "입사일": "EMPLOYEE.HIRE_DATE",
        "연봉": "EMPLOYEE.SALARY_ANNUAL",
        "성별": "EMPLOYEE.GENDER_CD",
        "전화번호": "EMPLOYEE.PHONE_NO",
        "나이": "EMPLOYEE.BIRTH_DATE",
        "주민번호": "EMPLOYEE.RRN_FRONT",  # ⚠️ 뒷자리(RRN_BACK)는 금지
    },

    # DB 타입 (기본값: oracle)
    "_db_type": "oracle",
}


def load_schema() -> dict:
    """
    스키마를 로드

    1. db_config.json이 있으면 DB에서 동적 조회
    2. 없거나 연결 실패 시 DEFAULT_SCHEMA 사용 (폴백)

    Returns:
        스키마 딕셔너리
    """
    config_path = _os.path.join(_os.path.dirname(__file__), "db_config.json")

    if _os.path.exists(config_path):
        try:
            from db_schema import get_schema
            schema = get_schema(config_path)
            print(f"[INFO] DB에서 스키마 로드 완료 (테이블 수: {len(schema['tables'])})")
            return schema
        except ImportError as e:
            print(f"[WARNING] DB 드라이버 미설치: {e}")
            print("[INFO] 기본 스키마(DEFAULT_SCHEMA) 사용")
            return DEFAULT_SCHEMA.copy()
        except Exception as e:
            print(f"[WARNING] DB 스키마 로딩 실패: {e}")
            print("[INFO] 기본 스키마(DEFAULT_SCHEMA) 사용")
            return DEFAULT_SCHEMA.copy()
    else:
        print("[INFO] db_config.json 없음 - 기본 스키마(DEFAULT_SCHEMA) 사용")
        return DEFAULT_SCHEMA.copy()


def refresh_schema(force: bool = True) -> dict:
    """
    스키마를 새로고침
    외부에서 호출 가능 (예: Streamlit UI에서 새로고침 버튼)

    Parameters:
        force: True면 캐시를 무시하고 DB에서 새로 조회

    Returns:
        새로고침된 스키마 딕셔너리
    """
    global SCHEMA

    config_path = _os.path.join(_os.path.dirname(__file__), "db_config.json")

    if _os.path.exists(config_path):
        try:
            from db_schema import get_schema
            SCHEMA = get_schema(config_path, force_refresh=force)
            print(f"[INFO] 스키마 새로고침 완료 (테이블 수: {len(SCHEMA['tables'])})")
        except Exception as e:
            print(f"[WARNING] 스키마 새로고침 실패: {e}")
            SCHEMA = DEFAULT_SCHEMA.copy()
    else:
        SCHEMA = DEFAULT_SCHEMA.copy()

    return SCHEMA


def get_current_db_type() -> str:
    """
    현재 로드된 스키마의 DB 타입을 반환

    Returns:
        "oracle" 또는 "mariadb"
    """
    return SCHEMA.get("_db_type", "oracle")


# 모듈 로드 시 스키마 초기화
SCHEMA = load_schema()


# ============================================================================
# 섹션 2: 보안 정책 상수 정의
# ============================================================================
"""
개인정보보호(PII) 및 보안을 위한 설정값들
"""

# 직접 조회가 금지된 컬럼 목록 (개인정보)
# 주민번호 뒷자리는 절대로 직접 SELECT 할 수 없음
PII_DENY_COLUMNS = {"EMPLOYEE.RRN_BACK"}

# 주민번호를 마스킹하여 출력하는 표현식
# 예: 900101-******* 형태로 출력
PII_MASK_EXPR = "e.rrn_front || '-' || RPAD('*', 7, '*') AS rrn_masked"

# 한 번에 조회 가능한 최대 행 수
# 대량 데이터 유출 방지 목적
MAX_ROWS = 10000


# ============================================================================
# 섹션 3: 유틸리티 함수 - 스키마 정보를 텍스트로 변환
# ============================================================================
def schema_compact_text() -> str:
    """
    스키마 정보를 LLM에게 전달하기 위한 간결한 텍스트 형식으로 변환

    [토큰 최적화 관련]
    이 함수의 반환값은 v1.0에서는 3번 전송되었지만,
    v2.0에서는 1번만 전송(약 200토큰 × 2회 = 400토큰 절감)

    반환 예시:
    - DEPARTMENT(DEPT_ID, DEPT_NAME, CREATED_AT)
    - EMPLOYEE(EMP_ID, EMP_NAME, DEPT_ID, ...)
    - DEPT_SALES_DAILY(SALES_DATE, DEPT_ID, REVENUE_AMT)
    """
    lines = []
    for table_name, info in SCHEMA["tables"].items():
        # 테이블명(컬럼1, 컬럼2, ...) 형태로 만듦
        lines.append(f"- {table_name}({', '.join(info['columns'])})")
    return "\n".join(lines)


def join_rules_text() -> str:
    """
    조인 규칙을 LLM에게 전달하기 위한 텍스트 형식으로 변환

    반환 예시:
    - EMPLOYEE.DEPT_ID = DEPARTMENT.DEPT_ID
    - DEPT_SALES_DAILY.DEPT_ID = DEPARTMENT.DEPT_ID
    """
    return "\n".join([f"- {j['left']} = {j['right']}" for j in SCHEMA["joins"]])


# ============================================================================
# 섹션 4: LLM 헬퍼 함수 - JSON 파싱
# ============================================================================
def extract_first_json(text: str) -> str:
    """
    LLM 응답에서 첫 번째 JSON 객체({ ... })를 추출

    LLM은 종종 JSON 앞뒤에 설명 텍스트나 마크다운 코드블록(```json ... ```)을
    붙여서 응답 이 함수는 그런 불필요한 부분을 제거하고
    순수 JSON만 추출

    매개변수:
        text: LLM의 원본 응답 문자열

    반환값:
        순수 JSON 문자열 (예: {"intent": "LIST", ...})

    예외:
        ValueError: JSON을 찾을 수 없는 경우
    """
    if not text:
        raise ValueError("LLM response is empty")

    t = text.strip()

    # 마크다운 코드 펜스 제거: ```json 또는 ```
    t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*```$", "", t)

    # JSON 객체의 시작({)과 끝(}) 위치 찾기
    start = t.find("{")
    end = t.rfind("}")

    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"Cannot find JSON object in LLM response: {t[:200]}")

    return t[start : end + 1]


# ============================================================================
# 토큰 사용량 추적 및 에러 처리
# ============================================================================

# 토큰 사용량 추적을 위한 전역 변수
_token_usage = {
    "input_tokens": 0,
    "output_tokens": 0,
    "total_tokens": 0,
    "llm_calls": 0
}

# LLM별 토큰 정책 정보
LLM_TOKEN_POLICIES = {
    "gemini": {
        "name": "Google Gemini",
        "free_tier": "무료: 분당 15 요청, 일일 1,500 요청",
        "paid_tier": "유료: 분당 1,000 요청, 무제한 토큰",
        "pricing_url": "https://ai.google.dev/pricing"
    },
    "openai": {
        "name": "OpenAI GPT",
        "free_tier": "무료 티어 없음 (크레딧 소진 시 중단)",
        "paid_tier": "유료: 모델별 토큰당 과금 (GPT-4: $0.03/1K input)",
        "pricing_url": "https://openai.com/pricing"
    },
    "anthropic": {
        "name": "Anthropic Claude",
        "free_tier": "무료 티어 없음 (크레딧 소진 시 중단)",
        "paid_tier": "유료: 모델별 토큰당 과금 (Claude 3: $0.015/1K input)",
        "pricing_url": "https://www.anthropic.com/pricing"
    }
}

# 토큰 사용량에 따른 예외처리
class TokenLimitError(Exception):
    """토큰 한도 초과 에러"""
    def __init__(self, message: str, llm_type: str = "gemini", original_error: Exception = None):
        self.message = message
        self.llm_type = llm_type
        self.original_error = original_error
        self.policy = LLM_TOKEN_POLICIES.get(llm_type, LLM_TOKEN_POLICIES["gemini"])
        super().__init__(self.message)

    def get_detail_message(self) -> str:
        return f"""현재 LLM API KEY에 대한 잔여 토큰이 없습니다.

[{self.policy['name']} 토큰 정책]
• {self.policy['free_tier']}
• {self.policy['paid_tier']}
• 요금 정보: {self.policy['pricing_url']}

[해결 방법]
1. 잠시 후 다시 시도하세요 (Rate Limit인 경우)
2. API 콘솔에서 사용량을 확인하세요

*API 토큰 정책은 LLM 제공업체에 따라 다릅니다. URL을 참고하여 자세한 내용을 확인하세요."""

def reset_token_usage():
    global _token_usage
    _token_usage = {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "llm_calls": 0
    }


def get_token_usage() -> dict:
    # 현재까지의 토큰 사용량 반환
    return _token_usage.copy()


def _detect_llm_type(error_message: str) -> str:
    # 에러 메시지에서 LLM 타입 감지
    error_lower = error_message.lower()
    if "gemini" in error_lower or "google" in error_lower:
        return "gemini"
    elif "openai" in error_lower or "gpt" in error_lower:
        return "openai"
    elif "anthropic" in error_lower or "claude" in error_lower:
        return "anthropic"
    return "gemini"  # 기본값


def _is_token_limit_error(error: Exception) -> bool:
    # 토큰/Rate Limit 에러인지 확인
    error_str = str(error).lower()
    limit_keywords = [
        "rate limit", "ratelimit", "quota", "exceeded",
        "429", "resource exhausted", "too many requests",
        "insufficient_quota", "billing", "credit"
    ]
    return any(keyword in error_str for keyword in limit_keywords)


def llm_json(llm, prompt: str) -> dict:
    """
    LLM에 프롬프트를 보내고, 응답에서 JSON을 추출하여 딕셔너리로 반환

    [토큰 최적화 관련]
    토큰은 기본적으로 1~2번만 호출

    처리 흐름:
    1. LLM에 프롬프트 전송
    2. 응답에서 JSON 추출
    3. JSON 문자열을 파이썬 딕셔너리로 변환
    4. 토큰 사용량 누적

    매개변수:
        llm: LangChain LLM 인스턴스
        prompt: LLM에 보낼 프롬프트 문자열

    반환값:
        파싱된 JSON 딕셔너리

    예외:
        TokenLimitError: 토큰 한도 초과 시
    """
    global _token_usage
    t0 = time.time()  # 시작 시간 기록 (디버깅용)

    try:
        # LLM 호출
        resp = llm.invoke(prompt)
        content = (resp.content or "").strip()

        # 토큰 사용량 추적 (Gemini API)
        if hasattr(resp, 'response_metadata') and resp.response_metadata:
            usage = resp.response_metadata.get('usage_metadata', {})
            _token_usage["input_tokens"] += usage.get('prompt_token_count', 0)
            _token_usage["output_tokens"] += usage.get('candidates_token_count', 0)
            _token_usage["total_tokens"] += usage.get('total_token_count', 0)
        _token_usage["llm_calls"] += 1

        dt = time.time() - t0  # 소요 시간 계산
        # 디버깅이 필요하면 아래 주석 해제:
        # print(f"[LLM] {dt:.2f}s tokens={_token_usage['total_tokens']}")

        # JSON 추출 및 파싱
        json_text = extract_first_json(content)
        return json.loads(json_text)

    except Exception as e:
        # 토큰/Rate Limit 에러 확인
        if _is_token_limit_error(e):
            llm_type = _detect_llm_type(str(e))
            raise TokenLimitError(
                message="현재 LLM API KEY에 대한 잔여 토큰이 없습니다.",
                llm_type=llm_type,
                original_error=e
            )
        # 그 외 에러는 그대로 전파
        raise


# ============================================================================
# 섹션 5: SQL 검증기 (Validator)
# ============================================================================
"""
생성된 SQL이 보안 정책을 준수하는지 검증하는 함수들

[중요] 이 섹션은 LLM을 호출하지 않고 규칙 기반으로 검증
       따라서 토큰 비용이 발생하지 않음
"""

# 금지 키워드 패턴 (DML/DDL 명령어)
# 이 패턴에 매칭되면 SQL 실행을 차단함
DENY_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b",
    re.IGNORECASE
)

# SELECT * 패턴 (금지)
# 명시적으로 컬럼을 지정해야 함
SELECT_STAR = re.compile(r"SELECT\s+\*", re.IGNORECASE)

# SQL 예약어/함수 목록
# 스키마 검증 시 이 단어들은 테이블/컬럼으로 간주하지 않음
# Oracle + MariaDB 공통 및 개별 키워드 포함
SQL_STOPWORDS = {
    # 공통 SQL 키워드
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER",
    "ON", "GROUP", "BY", "HAVING", "ORDER", "FETCH", "FIRST", "ROWS", "ONLY",
    "AS", "AND", "OR", "NOT", "IN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
    "COUNT", "SUM", "AVG", "MIN", "MAX",
    "LAST", "DESC", "ASC", "WITH",
    "DISTINCT", "LIKE", "BETWEEN", "COALESCE", "ROUND",
    # Oracle 전용
    "TRUNC", "MONTHS_BETWEEN", "SYSDATE", "TO_DATE", "RPAD", "SUBSTR", "NULLS",
    "NVL", "ROWNUM", "DECODE", "TO_CHAR", "TO_NUMBER",
    # MariaDB/MySQL 전용
    "LIMIT", "OFFSET", "IFNULL", "NOW", "DATE_FORMAT", "STR_TO_DATE",
    "CONCAT", "CURDATE", "CURTIME", "DATEDIFF", "DATE_ADD", "DATE_SUB",
}


def normalize_sql(sql: str) -> str:
    """
    SQL 문자열을 정규화

    수행 작업:
    1. 마크다운 코드블록(```sql ... ```) 제거
    2. SQL 주석 제거 (/* ... */ 및 -- ...)
    3. 앞뒤 공백 제거

    매개변수:
        sql: 원본 SQL 문자열

    반환값:
        정규화된 SQL 문자열
    """
    if not sql:
        return ""

    s = sql.strip()

    # 마크다운 코드 펜스 제거
    s = re.sub(r"^```(?:sql)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)

    # 블록 주석 제거: /* ... */
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.S)

    # 라인 주석 제거: -- ...
    s = re.sub(r"--.*?$", "", s, flags=re.M)

    return s.strip()


def extract_select_aliases(sql: str) -> Set[str]:
    """
    SELECT 절에서 정의된 별칭(alias)을 추출

    예: SELECT SUM(revenue) AS total_revenue
    → {"TOTAL_REVENUE"}를 반환

    별칭은 스키마 검증에서 제외해야 하므로 별도로 추출

    매개변수:
        sql: SQL 문자열

    반환값:
        별칭 집합 (대문자)
    """
    aliases = set()
    # AS 키워드 뒤에 오는 식별자를 찾음
    for m in re.finditer(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b", sql, flags=re.IGNORECASE):
        aliases.add(m.group(1).upper())
    return aliases


def extract_identifiers(sql: str) -> Set[str]:
    """
    SQL에서 모든 식별자(테이블명, 컬럼명 등)를 추출
    SQL 예약어는 제외

    매개변수:
        sql: SQL 문자열

    반환값:
        식별자 집합
    """
    # 알파벳/밑줄로 시작하고 알파벳/숫자/밑줄/점으로 구성된 토큰 찾기
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_\.]*", sql)
    ids = set()
    for t in tokens:
        # SQL 예약어는 제외
        if t.upper() in SQL_STOPWORDS:
            continue
        ids.add(t)
    return ids


def build_allowed_identifiers(schema: Dict[str, Any]) -> tuple:
    """
    스키마에서 허용된 테이블명과 컬럼명 목록을 생성

    매개변수:
        schema: 스키마 딕셔너리

    반환값:
        (허용된 테이블 집합, 허용된 컬럼 집합) 튜플
    """
    allowed_tables = set(schema["tables"].keys())
    allowed_cols = set()

    for table_name, info in schema["tables"].items():
        for col in info["columns"]:
            # "TABLE.COLUMN" 형태와 "COLUMN" 형태 모두 허용
            allowed_cols.add(f"{table_name}.{col}".upper())
            allowed_cols.add(col.upper())

    return allowed_tables, allowed_cols


def validate_sql(sql: str, max_rows: int = 10000) -> Dict[str, Any]:
    """
    생성된 SQL이 보안 정책을 준수하는지 검증

    [중요] 이 함수는 LLM을 호출하지 않음
           규칙 기반 검증으로 토큰 비용이 발생하지 않음

    검증 항목:
    1. DML/DDL 키워드 금지 (INSERT, UPDATE, DELETE 등)
    2. SELECT * 금지 (명시적 컬럼 지정 필요)
    3. 최대 행 수 제한 필수 (FETCH FIRST N ROWS ONLY)
    4. PII 컬럼 직접 참조 금지 (RRN_BACK)
    5. 스키마에 없는 테이블/컬럼 사용 금지

    매개변수:
        sql: 검증할 SQL 문자열
        max_rows: 최대 허용 행 수 (기본값: 10000)

    반환값:
        {
            "passed": True/False,  # 검증 통과 여부
            "errors": [...],       # 에러 메시지 목록
            "warnings": [...]      # 경고 메시지 목록
        }
    """
    errors: List[str] = []
    warnings: List[str] = []

    # SQL 정규화 (주석, 코드블록 제거)
    s = normalize_sql(sql)

    if not s:
        return {"passed": False, "errors": ["SQL이 비어있습니다."], "warnings": warnings}

    # -----------------------------------------------------------------------
    # 검증 1: DML/DDL 키워드 금지
    # -----------------------------------------------------------------------
    if DENY_KEYWORDS.search(s):
        errors.append("DML/DDL 키워드가 포함되어 있습니다(SELECT만 허용).")

    # -----------------------------------------------------------------------
    # 검증 2: SELECT * 금지
    # -----------------------------------------------------------------------
    if SELECT_STAR.search(s):
        errors.append("SELECT * 는 금지입니다(컬럼을 명시해야 함).")

    # -----------------------------------------------------------------------
    # 검증 3: 최대 행 수 제한 확인
    # DB 타입에 따라 다른 구문 검사:
    # - Oracle: FETCH FIRST N ROWS ONLY 또는 ROWNUM <= N
    # - MariaDB: LIMIT N
    # -----------------------------------------------------------------------
    db_type = get_current_db_type()

    # Oracle 방식
    oracle_limit = (
        re.search(r"FETCH\s+FIRST\s+\d+\s+ROWS\s+ONLY", s, re.IGNORECASE) or
        re.search(r"ROWNUM\s*<=\s*\d+", s, re.IGNORECASE)
    )

    # MariaDB/MySQL 방식
    mariadb_limit = re.search(r"\bLIMIT\s+\d+", s, re.IGNORECASE)

    if db_type in ("mariadb", "mysql"):
        # MariaDB: LIMIT 또는 Oracle 방식 모두 허용
        limit_ok = oracle_limit or mariadb_limit
        limit_hint = f"LIMIT {max_rows}"
    else:
        # Oracle: Oracle 방식만 허용
        limit_ok = oracle_limit
        limit_hint = f"FETCH FIRST {max_rows} ROWS ONLY 또는 ROWNUM <= {max_rows}"

    if not limit_ok:
        errors.append(f"최대 조회 건수 제한이 없습니다({limit_hint} 필요).")
    else:
        # FETCH FIRST 방식의 제한 확인
        m = re.search(r"FETCH\s+FIRST\s+(\d+)\s+ROWS\s+ONLY", s, re.IGNORECASE)
        if m and int(m.group(1)) > max_rows:
            errors.append(f"조회 제한이 {max_rows}건을 초과")

        # ROWNUM 방식의 제한 확인
        m2 = re.search(r"ROWNUM\s*<=\s*(\d+)", s, re.IGNORECASE)
        if m2 and int(m2.group(1)) > max_rows:
            errors.append(f"ROWNUM 제한이 {max_rows}건을 초과")

        # LIMIT 방식의 제한 확인 (MariaDB)
        m3 = re.search(r"\bLIMIT\s+(\d+)", s, re.IGNORECASE)
        if m3 and int(m3.group(1)) > max_rows:
            errors.append(f"LIMIT 제한이 {max_rows}건을 초과")

    # -----------------------------------------------------------------------
    # 검증 4: PII(개인정보) 직접 참조 금지
    # 주민번호 뒷자리(RRN_BACK)는 직접 조회 불가
    # -----------------------------------------------------------------------
    for deny in PII_DENY_COLUMNS:
        col = deny.split(".")[1]  # "EMPLOYEE.RRN_BACK" → "RRN_BACK"
        if re.search(rf"\b{col}\b", s, re.IGNORECASE) or re.search(rf"\b{deny}\b", s, re.IGNORECASE):
            errors.append("주민등록번호 뒷자리(RRN_BACK) 직접 출력/참조는 금지입니다(마스킹만 허용).")
            break

    # -----------------------------------------------------------------------
    # 검증 5: 스키마 식별자 검사
    # SQL에 사용된 테이블/컬럼이 스키마에 존재하는지 확인
    # -----------------------------------------------------------------------
    allowed_tables, allowed_cols = build_allowed_identifiers(SCHEMA)

    # 허용되는 테이블 별칭 (e, d, s)
    alias_ok = {"E", "D", "S"}

    # SELECT 절의 별칭 (스키마 검증에서 제외)
    select_aliases = extract_select_aliases(s)

    # SQL에서 추출한 모든 식별자
    ids = extract_identifiers(s)

    for ident in ids:
        up = ident.upper()

        # 테이블 별칭은 통과
        if up in alias_ok:
            continue

        # SELECT 절 별칭은 통과
        if up in select_aliases:
            continue

        # 허용된 테이블명이면 통과
        if up in allowed_tables:
            continue

        # 허용된 컬럼명이면 통과
        if up in allowed_cols:
            continue

        # "alias.column" 형태면 column만 확인
        if "." in up:
            col = up.split(".")[-1]
            if col in allowed_cols:
                continue

        # 위 조건을 모두 통과하지 못하면 에러
        errors.append(f"스키마에 없는 식별자(테이블/컬럼)로 추정: {ident}")

    return {"passed": len(errors) == 0, "errors": errors, "warnings": warnings}


# ============================================================================
# 섹션 6: 메모리 및 날짜 파싱 함수
# ============================================================================
"""
사용자와의 대화에서 날짜 정보를 추출하고 메모리에 저장하는 기능
다중 턴 대화에서 컨텍스트를 유지하기 위해 사용됨

[중요] 이 섹션도 LLM을 호출하지 않음
       규칙 기반 파싱으로 토큰 비용이 발생하지 않음
"""

# 날짜 패턴: YYYY-MM-DD 또는 YYYY/MM/DD 또는 YYYY.MM.DD
DATE_RE = re.compile(r"(\d{4})[-/.](\d{2})[-/.](\d{2})")


def parse_dates_from_text(text: str) -> List[str]:
    """
    텍스트에서 날짜를 추출

    지원 형식:
    - 2025-01-01
    - 2025/01/01
    - 2025.01.01

    매개변수:
        text: 검색할 텍스트

    반환값:
        추출된 날짜 목록 (YYYY-MM-DD 형식)

    예시:
        parse_dates_from_text("2025-01-01부터 2025-01-31까지")
        → ["2025-01-01", "2025-01-31"]
    """
    dates = []
    for m in DATE_RE.finditer(text):
        y, mo, d = m.group(1), m.group(2), m.group(3)
        dates.append(f"{y}-{mo}-{d}")
    return dates


def update_memory_from_user_text(memory: Dict[str, Any], text: str) -> Dict[str, Any]:
    """
    사용자 입력에서 날짜를 추출하여 메모리에 저장

    저장 규칙:
    - 날짜 2개: 기간으로 저장 (:p_from_date, :p_to_date)
    - 날짜 1개 + "입사"/"이후" 키워드: 입사 기준일로 저장 (:p_hire_from)
    - 날짜 1개 (그 외): 시작일로 저장 (:p_from_date)

    매개변수:
        memory: 현재 메모리 딕셔너리
        text: 사용자 입력 텍스트

    반환값:
        업데이트된 메모리 딕셔너리

    예시:
        update_memory_from_user_text({}, "2025-01-01 ~ 2025-01-31")
        → {":p_from_date": "2025-01-01", ":p_to_date": "2025-01-31"}
    """
    dates = parse_dates_from_text(text)

    if len(dates) >= 2:
        # 날짜 2개: 기간으로 저장
        memory[":p_from_date"] = dates[0]
        memory[":p_to_date"] = dates[1]
    elif len(dates) == 1:
        # 날짜 1개: 문맥에 따라 결정
        if ("입사" in text) or ("이후" in text):
            memory[":p_hire_from"] = dates[0]
        else:
            memory[":p_from_date"] = dates[0]

    return memory


# ============================================================================
# 섹션 7: LangGraph 상태 정의
# ============================================================================
"""
BotState는 LangGraph 워크플로우에서 각 노드 간에 전달되는 상태 객체
TypedDict를 사용하여 타입 힌트를 제공

[v2.0 변경사항]
- parsed 필드: 여전히 존재하지만, generate_sql_node에서 함께 추출
- plan 필드: generate_sql_node에서 함께 추출 (별도 노드 없음)
"""

class BotState(TypedDict):
    """
    챗봇의 상태를 나타내는 타입 정의

    각 노드는 이 상태를 받아서 처리하고, 수정된 상태를 반환
    """
    # -----------------------------------------------------------------------
    # 입력 데이터
    # -----------------------------------------------------------------------
    user_question: str                    # 사용자의 질문
    chat_history: List[Dict[str, str]]    # 대화 기록 [{"role": "user/assistant", "content": "..."}]
    memory: Dict[str, Any]                # 슬롯 메모리 (날짜, 기간 등 저장)

    # -----------------------------------------------------------------------
    # 처리 중간 결과
    # [v2.0] 이 필드들은 generate_sql_node에서 한 번에 채워짐
    # -----------------------------------------------------------------------
    parsed: Dict[str, Any]      # 의도 파싱 결과 (intent, topic 등)
    plan: Dict[str, Any]        # 쿼리 계획 (select, from, where 등)
    sql: Dict[str, Any]         # 생성된 SQL (query, binds)
    validation: Dict[str, Any]  # 검증 결과 (passed, errors, warnings)

    # -----------------------------------------------------------------------
    # 복구(재시도) 관련
    # -----------------------------------------------------------------------
    repair: Dict[str, Any]  # {"attempt": 현재 시도 횟수, "max_attempt": 최대 시도 횟수}

    # -----------------------------------------------------------------------
    # 추가 질문 필요 여부
    # -----------------------------------------------------------------------
    need_clarification: bool       # True면 추가 질문 필요
    clarification_question: str    # 사용자에게 할 추가 질문


# ============================================================================
# 섹션 8: LLM 인스턴스 생성
# ============================================================================
"""
Google Gemini 모델을 사용하기 위한 LangChain 인스턴스 생성

참고:
- 모델명은 계정/권한에 따라 다를 수 있음
- 동작하지 않으면 "gemini-1.5-flash"로 변경하여 테스트
- temperature=0.1: 낮은 값일수록 일관된 응답 (SQL 생성에 적합)
"""
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",  # 사용할 모델 이름
    temperature=0.1,           # 창의성 수준 (0~1, 낮을수록 일관됨)
)


# ============================================================================
# 섹션 9: 노드 - clarify (명확화 확인)
# ============================================================================
def clarify_node(state: BotState) -> BotState:
    """
    사용자 질문이 SQL 생성에 충분한 정보를 포함하는지 확인

    [중요] 이 노드는 LLM을 호출하지 않음
           규칙 기반 검사로 토큰 비용이 발생하지 않음

    확인 항목:
    1. 실적/매출/관계/상관 키워드 → 기간 정보 필요
    2. "입사" + "이후/부터" → 입사 기준일 필요

    또한 사용자 입력에서 날짜를 추출하여 메모리에 저장

    처리 흐름:
    1. 사용자 입력에서 날짜 추출 → 메모리 업데이트
    2. 질문에 기간이 필요한 키워드가 있는지 확인
    3. 메모리에 필요한 정보가 있는지 확인
    4. 정보가 부족하면 추가 질문 설정

    매개변수:
        state: 현재 봇 상태

    반환값:
        업데이트된 봇 상태
    """
    q = state["user_question"]
    memory = state["memory"]

    # 사용자 입력에서 날짜 파싱 후 메모리 업데이트
    state["memory"] = update_memory_from_user_text(memory, q)

    # 기간이 필요한 키워드 확인
    need_period = any(k in q for k in ["실적", "매출", "관계", "상관"])

    # 입사 기준일이 필요한 패턴 확인
    need_hire_from = ("입사" in q) and ("이후" in q or "부터" in q)

    # 메모리에 필요한 정보가 있는지 확인
    has_period = (":p_from_date" in state["memory"]) and (":p_to_date" in state["memory"])
    has_hire = (":p_hire_from" in state["memory"])

    # 기본값: 추가 질문 불필요
    state["need_clarification"] = False
    state["clarification_question"] = ""

    # 기간이 필요하지만 없는 경우
    if need_period and not has_period:
        state["need_clarification"] = True
        state["clarification_question"] = "기간이 필요 예: 2025-01-01 ~ 2025-01-31"
        return state

    # 입사 기준일이 필요하지만 없는 경우
    if need_hire_from and not has_hire:
        state["need_clarification"] = True
        state["clarification_question"] = "입사일 기준 날짜가 필요 예: 2024-01-01 이후"
        return state

    return state


# ============================================================================
# 섹션 10: 노드 - generate_sql (통합 노드) ⭐ v2.0 핵심 변경
# ============================================================================
def generate_sql_node(state: BotState) -> BotState:
    """
    ============================================================================
    [v2.0 통합 노드] 의도 파싱 + 쿼리 계획 + SQL 생성을 한 번의 LLM 호출로 처리
    ============================================================================

    [토큰 최적화 핵심]
    기존 v1.0에서는 3개의 노드가 각각 LLM을 호출했습니다:
    - parse_intent_node: 의도 파싱 (LLM 호출 #1)
    - build_plan_node: 쿼리 계획 (LLM 호출 #2)
    - generate_sql_node: SQL 생성 (LLM 호출 #3)

    v2.0에서는 이 3개를 하나로 통합하여:
    - LLM 호출 횟수: 3회 → 1회
    - 스키마/조인규칙 전송: 3회 → 1회
    - 예상 토큰 절감: 약 50~60%

    [프롬프트 구조 설명]
    프롬프트는 다음 섹션으로 구성됩니다:

    1. 역할 정의
       - LLM에게 Oracle SQL 생성기 역할 부여
       - JSON 형식으로만 응답하도록 지시

    2. 사용자 질문
       - 원본 질문 텍스트 전달

    3. 메모리 (바인드 변수)
       - 이전 대화에서 추출된 날짜/기간 정보
       - 예: {":p_from_date": "2025-01-01", ":p_to_date": "2025-01-31"}

    4. 스키마 정보
       - 테이블 목록과 컬럼 정보
       - LLM이 유효한 테이블/컬럼만 사용하도록 함

    5. 조인 규칙
       - 테이블 간 연결 관계
       - LLM이 올바른 JOIN 조건을 생성하도록 함

    6. 동의어 매핑
       - 한국어 용어 → 실제 테이블/컬럼 매핑
       - 예: "매출" → "DEPT_SALES_DAILY.REVENUE_AMT"

    7. 보안 정책
       - SELECT만 허용 (DML/DDL 금지)
       - SELECT * 금지
       - 행 수 제한 필수
       - PII 마스킹 규칙

    8. 출력 형식
       - JSON 스키마 정의
       - intent, sql, binds 필드 포함

    매개변수:
        state: 현재 봇 상태

    반환값:
        parsed, plan, sql 필드가 업데이트된 봇 상태
    """
    memory = state["memory"]
    user_question = state["user_question"]

    # =========================================================================
    # 통합 프롬프트 구성
    # =========================================================================
    # [주의] 이 프롬프트가 토큰 비용의 대부분을 차지
    #        불필요한 내용은 제거하고, 필수 정보만 포함
    # =========================================================================

    prompt = f"""You are an Oracle SQL generator. Analyze the question and generate SQL directly.
Return ONLY valid JSON. No prose or explanation.

=== USER QUESTION ===
{user_question}

=== MEMORY (bind values from previous conversation) ===
{json.dumps(memory, ensure_ascii=False)}

=== DATABASE SCHEMA ===
{schema_compact_text()}

=== JOIN RULES ===
{join_rules_text()}

=== KOREAN SYNONYMS ===
{json.dumps(SCHEMA["synonyms"], ensure_ascii=False)}

=== SECURITY POLICY (MUST FOLLOW) ===
1. SELECT only. No INSERT/UPDATE/DELETE/DROP/ALTER.
2. No SELECT *. Always specify columns explicitly.
3. MUST include: FETCH FIRST {MAX_ROWS} ROWS ONLY (or less for TOP queries).
4. If question asks for 주민번호, use masked format: {PII_MASK_EXPR}
5. NEVER reference EMPLOYEE.RRN_BACK directly.
6. Use bind variables for dates (e.g., :p_from_date, :p_to_date).
7. Use table aliases: department d, employee e, dept_sales_daily s

=== OUTPUT JSON FORMAT ===
{{
  "intent": "LIST|AGG|TOP|DUP_INFO|DUP_COUNT|CORR|OTHER",
  "topic": "EMPLOYEE|DEPARTMENT|SALES|MIXED|OTHER",
  "sql": "SELECT ... FROM ... FETCH FIRST N ROWS ONLY",
  "binds": [
    {{"name": ":p_from_date", "type": "DATE", "value": "2025-01-01", "desc": "시작일"}},
    {{"name": ":p_to_date", "type": "DATE", "value": "2025-01-31", "desc": "종료일"}}
  ]
}}

=== INTENT TYPES ===
- LIST: 목록 조회 (예: "모든 직원 보여줘")
- AGG: 집계 (예: "부서별 인원 수")
- TOP: 상위/하위 N건 (예: "연봉 높은 직원 5명")
- DUP_INFO: 중복 데이터 상세 조회
- DUP_COUNT: 중복 건수 조회
- CORR: 상관관계/비교 분석
- OTHER: 기타

Return JSON only. No markdown fences.
""".strip()

    # =========================================================================
    # LLM 호출 (1회)
    # =========================================================================
    # [v2.0] 여기서 1번의 LLM 호출로 의도 파싱 + 계획 + SQL 생성을 모두 처리
    # =========================================================================
    result = llm_json(llm, prompt)

    # =========================================================================
    # 결과 파싱 및 상태 업데이트
    # =========================================================================

    # parsed 필드: 의도 정보 저장 (기존 호환성 유지)
    state["parsed"] = {
        "intent": result.get("intent", "OTHER"),
        "topic": result.get("topic", "OTHER"),
    }

    # plan 필드: 쿼리 계획 (통합 버전에서는 간략화)
    state["plan"] = {
        "generated_by": "unified_node_v2",
        "binds": result.get("binds", [])
    }

    # sql 필드: 생성된 SQL과 바인드 변수
    state["sql"] = {
        "query": result.get("sql", ""),
        "binds": result.get("binds", [])
    }

    return state


# ============================================================================
# 섹션 11: 노드 - validate_sql (SQL 검증)
# ============================================================================
def validate_sql_node(state: BotState) -> BotState:
    """
    생성된 SQL이 보안 정책을 준수하는지 검증

    [중요] 이 노드는 LLM을 호출하지 않음
           규칙 기반 검증으로 토큰 비용이 발생하지 않음

    validate_sql() 함수를 호출하여 검증 수행

    매개변수:
        state: 현재 봇 상태

    반환값:
        validation 필드가 업데이트된 봇 상태
    """
    state["validation"] = validate_sql(state["sql"]["query"], max_rows=MAX_ROWS)
    return state


# ============================================================================
# 섹션 12: 노드 - repair (복구/재시도)
# ============================================================================
def repair_node(state: BotState) -> BotState:
    """
    SQL 검증 실패 시 복구를 시도

    [v2.0 동작 방식]
    검증 에러 메시지를 질문에 추가하여 LLM이 문제를 인식하고
    수정된 쿼리를 생성하도록 유도

    복구 흐름:
    1. 검증 에러 메시지 추출
    2. 원래 질문 + 에러 메시지를 합쳐서 새 질문 구성
    3. generate_sql_node로 다시 전달 (LLM 재호출)

    예시:
    원래 질문: "모든 직원 보여줘"
    에러: "SELECT * 는 금지입니다"

    수정된 질문:
    "모든 직원 보여줘

    [VALIDATION_ERRORS_TO_FIX]
    - SELECT * 는 금지입니다"

    → LLM이 에러를 보고 SELECT * 대신 명시적 컬럼을 사용

    매개변수:
        state: 현재 봇 상태

    반환값:
        user_question과 repair.attempt가 업데이트된 봇 상태
    """
    # 재시도 횟수 증가
    state["repair"]["attempt"] += 1

    # 검증 에러 가져오기
    errs = state["validation"]["errors"]

    # 에러를 질문 뒤에 추가하여 LLM이 인식하도록 함
    state["user_question"] = (
        state["user_question"] +
        "\n\n[VALIDATION_ERRORS_TO_FIX]\n- " +
        "\n- ".join(errs)
    )
    return state


def should_repair(state: BotState) -> str:
    """
    검증 실패 시 복구를 시도할지 결정

    결정 로직:
    1. 검증 통과 → "end" (종료)
    2. 최대 재시도 횟수 도달 → "end" (종료)
    3. 그 외 → "repair" (복구 시도)

    매개변수:
        state: 현재 봇 상태

    반환값:
        "end" 또는 "repair"
    """
    # 검증 통과했으면 종료
    if state["validation"]["passed"]:
        return "end"

    # 최대 재시도 횟수에 도달했으면 종료
    if state["repair"]["attempt"] >= state["repair"]["max_attempt"]:
        return "end"

    # 복구 시도
    return "repair"


# ============================================================================
# 섹션 13: LangGraph 그래프 구성 ⭐ v2.0 간소화
# ============================================================================
"""
LangGraph를 사용하여 노드들을 연결하고 워크플로우를 구성

[v2.0 그래프 구조] - 간소화됨!
================================================================================

v1.0 (기존):
    clarify → parse_intent → build_plan → generate_sql → validate_sql → repair
                   ↑              ↑            ↑
                [LLM #1]      [LLM #2]     [LLM #3]

v2.0 (최적화):
    clarify → generate_sql → validate_sql → repair
                   ↑                            ↓
               [LLM #1] ←───────────────────────┘
                                           [LLM #2 only if repair needed]

================================================================================

토큰 절감 효과:
- 정상 케이스: 3회 → 1회 LLM 호출 (약 66% 감소)
- 재시도 케이스: 5회 → 2회 LLM 호출 (약 60% 감소)
"""

# StateGraph 인스턴스 생성
graph = StateGraph(BotState)

# -----------------------------------------------------------------------
# 노드 추가
# [v2.0] parse_intent_node, build_plan_node 제거됨
# -----------------------------------------------------------------------
graph.add_node("clarify", clarify_node)           # 명확화 확인 (LLM 호출 없음)
graph.add_node("generate_sql", generate_sql_node) # 통합 노드 (LLM 호출 1회)
graph.add_node("validate_sql", validate_sql_node) # SQL 검증 (LLM 호출 없음)
graph.add_node("repair", repair_node)             # 복구/재시도

# -----------------------------------------------------------------------
# 시작점 설정
# -----------------------------------------------------------------------
graph.set_entry_point("clarify")

# -----------------------------------------------------------------------
# 엣지(연결) 설정
# -----------------------------------------------------------------------

# clarify 노드 이후 분기:
# - need_clarification=True → END (추가 질문 반환)
# - need_clarification=False → generate_sql로 진행
graph.add_conditional_edges(
    "clarify",
    lambda s: "need" if s["need_clarification"] else "go",
    {"need": END, "go": "generate_sql"},  # [v2.0] parse_intent 대신 generate_sql로 직행
)

# [v2.0] 순차적 연결 - 간소화됨
# 기존: parse_intent → build_plan → generate_sql → validate_sql
# 변경: generate_sql → validate_sql
graph.add_edge("generate_sql", "validate_sql")

# validate_sql 노드 이후 분기:
# - 검증 통과 또는 최대 재시도 도달 → END
# - 검증 실패 → repair로 이동
graph.add_conditional_edges("validate_sql", should_repair, {"repair": "repair", "end": END})

# repair 후에는 다시 generate_sql로 (통합 노드에서 재생성)
graph.add_edge("repair", "generate_sql")

# 그래프 컴파일 (실행 가능한 앱으로 변환)
app = graph.compile()


# ============================================================================
# 섹션 14: 외부 호출용 실행 함수
# ============================================================================
def run(
    question: str,
    chat_history: Optional[List[Dict[str, str]]] = None,
    memory: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    챗봇의 메인 실행 함수

    외부(streamlit_app.py 등)에서 이 함수를 호출하여 챗봇을 실행

    [v2.0 토큰 최적화]
    이 함수를 호출하면 내부적으로 LLM이 1~2회만 호출
    - 정상 케이스: 1회 (generate_sql_node)
    - 검증 실패 + 재시도: 최대 2회

    매개변수:
        question: 사용자의 질문 문자열
        chat_history: 대화 기록 (선택사항)
                     형식: [{"role": "user", "content": "..."}, ...]
        memory: 슬롯 메모리 (선택사항)
               형식: {":p_from_date": "2025-01-01", ...}

    반환값:
        {
            "need_clarification": bool,    # 추가 질문 필요 여부
            "clarification_question": str, # 추가 질문 내용
            "sql": {                       # 생성된 SQL
                "query": str,              # SQL 문자열
                "binds": [...]             # 바인드 변수 목록
            },
            "validation": {                # 검증 결과
                "passed": bool,
                "errors": [...],
                "warnings": [...]
            },
            "memory": {...},               # 업데이트된 메모리
            "parsed": {...},               # 파싱 결과 (intent, topic)
            "plan": {...}                  # 쿼리 계획 정보
        }

    사용 예시:
        # 첫 번째 질문
        result = run("실적이 가장 좋은 부서를 알려줘")

        if result["need_clarification"]:
            print(result["clarification_question"])
            # → "기간이 필요 예: 2025-01-01 ~ 2025-01-31"

            # 두 번째 질문 (기간 제공)
            result = run("2025-01-01 ~ 2025-01-31", memory=result["memory"])

        print(result["sql"]["query"])  # 생성된 SQL 출력
    """
    # 기본값 설정
    if chat_history is None:
        chat_history = []
    if memory is None:
        memory = {}

    # 토큰 사용량 초기화
    reset_token_usage()

    # 초기 상태 생성
    init_state: BotState = {
        "user_question": question,
        "chat_history": chat_history,
        "memory": memory,
        "parsed": {},
        "plan": {},
        "sql": {"query": "", "binds": []},
        "validation": {"passed": False, "errors": [], "warnings": []},
        "repair": {"attempt": 0, "max_attempt": 2},  # 최대 2회 재시도
        "need_clarification": False,
        "clarification_question": "",
    }

    # 그래프 실행
    out = app.invoke(init_state)

    # 결과 정리하여 반환
    return {
        "need_clarification": out.get("need_clarification", False),
        "clarification_question": out.get("clarification_question", ""),
        "sql": out.get("sql", {"query": "", "binds": []}),
        "validation": out.get("validation", {"passed": False, "errors": [], "warnings": []}),
        "memory": out.get("memory", memory),
        "parsed": out.get("parsed", {}),
        "plan": out.get("plan", {}),
        "token_usage": get_token_usage(),  # 토큰 사용량 정보 추가
    }


# ============================================================================
# 섹션 15: 직접 실행 시 테스트 코드
# ============================================================================
if __name__ == "__main__":
    """
    이 파일을 직접 실행하면 (python app.py) 테스트가 수행됨

    [v2.0 테스트 시나리오]
    1. "실적이 가장 좋은 부서를 알려줘" → 기간 추가 질문 (LLM 호출 없음)
    2. "2025-01-01 ~ 2025-01-31" → 기간 메모리에 저장 (LLM 호출 없음)
    3. "그 기간으로 실적이 가장 좋은 부서를 알려줘" → SQL 생성 (LLM 1회 호출)
    """
    print("=" * 70)
    print("Oracle SQL 생성 챗봇 - 콘솔 테스트 (v2.0 토큰 최적화 버전)")
    print("=" * 70)

    # 메모리 초기화
    mem = {}

    # 테스트 1: 기간 없이 실적 질문 → 추가 질문 예상 (LLM 호출 없음)
    print("\n[테스트 1] 질문: 실적이 가장 좋은 부서를 알려줘.")
    print("         → 기간이 필요하므로 추가 질문 (LLM 호출 없음)")
    out1 = run("실적이 가장 좋은 부서를 알려줘.", memory=mem)
    print(f"추가 질문 필요: {out1['need_clarification']}")
    print(f"추가 질문: {out1['clarification_question']}")
    mem = out1["memory"]

    # 테스트 2: 기간 제공 → 메모리에 저장 (LLM 호출 없음, 날짜만 파싱)
    print("\n[테스트 2] 답변: 2025-01-01 ~ 2025-01-31")
    print("         → 날짜 파싱하여 메모리 저장 (LLM 호출 없음)")
    out2 = run("2025-01-01 ~ 2025-01-31", memory=mem)
    print(f"추가 질문 필요: {out2['need_clarification']}")
    print(f"메모리: {out2['memory']}")
    mem = out2["memory"]

    # 테스트 3: 기간이 메모리에 있으므로 SQL 생성 (LLM 1회 호출)
    print("\n[테스트 3] 질문: 그 기간으로 실적이 가장 좋은 부서를 알려줘.")
    print("         → SQL 생성 (LLM 1회 호출 - 통합 노드)")
    out3 = run("그 기간으로 실적이 가장 좋은 부서를 알려줘.", memory=mem)
    print(f"\n의도(intent): {out3['parsed'].get('intent', 'N/A')}")
    print(f"주제(topic): {out3['parsed'].get('topic', 'N/A')}")
    print(f"\n생성된 SQL:\n{out3['sql']['query']}")
    print(f"\n바인드 변수: {json.dumps(out3['sql']['binds'], ensure_ascii=False, indent=2)}")
    print(f"\n검증 결과: {out3['validation']}")

    # 테스트 4: 단순 질문 (기간 불필요) - LLM 1회 호출
    print("\n" + "=" * 70)
    print("[테스트 4] 질문: 부서별 인원 수를 알려줘")
    print("         → SQL 생성 (LLM 1회 호출 - 통합 노드)")
    out4 = run("부서별 인원 수를 알려줘")
    print(f"\n의도(intent): {out4['parsed'].get('intent', 'N/A')}")
    print(f"\n생성된 SQL:\n{out4['sql']['query']}")
    print(f"\n검증 결과: {out4['validation']}")
