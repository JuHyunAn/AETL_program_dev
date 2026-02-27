"""
================================================================================
AETL Agent Core  —  LangGraph 기반 Tool-calling ETL 에이전트
================================================================================
자연어 요청을 받아 ETL 관련 태스크를 자동으로 수행합니다.

지원 태스크:
  - 테이블 스키마 조회
  - 데이터 프로파일 분석
  - 검증 쿼리 자동 생성
  - 검증 규칙 자동 제안
  - 소스/타겟 건수 비교
  - 테이블 검색

Architecture:
  사용자 메시지 → [Agent Node] → LLM Tool-calling → [Tool Execute Node] → 반복 → 응답
================================================================================
"""

from __future__ import annotations

import json
import os
from typing import Annotated, Any, Sequence

from dotenv import load_dotenv
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

load_dotenv()

# ─────────────────────────────────────────────────────────────
# State 정의
# ─────────────────────────────────────────────────────────────
class AETLState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    db_type: str           # oracle | mariadb | postgresql
    config_path: str       # db_config.json 경로


# ─────────────────────────────────────────────────────────────
# Tool 정의
# ─────────────────────────────────────────────────────────────

@tool
def get_table_schema(table_name: str) -> str:
    """
    DB에서 테이블의 스키마(컬럼, 타입, PK/FK 정보)를 조회합니다.
    table_name: 조회할 테이블명 (대소문자 무관)
    """
    # 1) 메타데이터 우선 조회 (빠름 — SQLite)
    try:
        from aetl_metadata_engine import get_table_schema_from_meta
        meta = get_table_schema_from_meta(table_name)
        if meta:
            return json.dumps(meta, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # 2) fallback: db_schema.get_schema() — 캐시(.schema_cache.json) 활용
    try:
        from db_schema import get_schema
        schema = get_schema("db_config.json", force_refresh=False)

        tbl_upper = table_name.upper()
        tables = schema.get("tables", {})
        matched = None
        for k in tables:
            if k.upper() == tbl_upper:
                matched = k
                break

        if not matched:
            available = ", ".join(str(k) for i, k in enumerate(tables) if i < 20)
            return f"테이블 '{table_name}'을 찾을 수 없습니다. 사용 가능한 테이블 예시: {available}"

        info = tables[matched]
        result = {
            "table_name": matched,
            "columns": info.get("columns", []),
            "pk": info.get("pk", []),
            "fk": info.get("fk", []),
        }
        return json.dumps(result, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"스키마 조회 오류: {e}"


@tool
def search_tables(keyword: str) -> str:
    """
    테이블명에 키워드가 포함된 테이블 목록을 검색합니다.
    keyword: 검색 키워드 (테이블명 일부)
    """
    # 1) 메타데이터 우선 조회
    try:
        from aetl_metadata_engine import search_tables_from_meta, is_schema_synced
        if is_schema_synced():
            matches = search_tables_from_meta(keyword)
            if matches:
                return f"검색 결과 ({len(matches)}개):\n" + "\n".join(f"  - {t}" for t in matches[:30])
            return f"키워드 '{keyword}'로 검색된 테이블이 없습니다. (메타데이터 기준)"
    except Exception:
        pass

    # 2) fallback: db_schema.get_schema() — 캐시 활용
    try:
        from db_schema import get_schema
        schema = get_schema("db_config.json", force_refresh=False)
        kw = keyword.upper()
        matches = [t for t in schema.get("tables", {}) if kw in t.upper()]
        if not matches:
            return f"키워드 '{keyword}'로 검색된 테이블이 없습니다."
        return f"검색 결과 ({len(matches)}개):\n" + "\n".join(f"  - {t}" for t in matches[:30])
    except Exception as e:
        return f"테이블 검색 오류: {e}"


@tool
def profile_table_tool(table_name: str) -> str:
    """
    DB 테이블의 데이터 프로파일(건수, NULL 비율, 유니크 값 수, 최소/최대값, 도메인 추론)을 분석합니다.
    table_name: 프로파일링할 테이블명
    """
    # 1) 메타데이터 우선 조회 (즉시 응답)
    try:
        from aetl_metadata_engine import get_profile_from_meta
        from aetl_profiler import profile_summary_text
        cached = get_profile_from_meta(table_name)
        if cached:
            return "[메타데이터 캐시]\n" + profile_summary_text(cached)
    except Exception:
        pass

    # 2) fallback: 라이브 프로파일링 (느릴 수 있음)
    try:
        from aetl_profiler import profile_table_from_config, profile_summary_text
        profile = profile_table_from_config("db_config.json", table_name, top_n=5)
        return "[라이브 DB 조회]\n" + profile_summary_text(profile)
    except Exception as e:
        return (
            f"프로파일링 오류: {e}\n"
            "메타데이터가 없고 DB 직접 조회도 실패했습니다. "
            "sync_metadata_tool로 먼저 메타데이터를 동기화하세요."
        )


@tool
def generate_validation_queries_tool(
    source_table: str,
    target_table: str,
    db_type: str = "oracle",
) -> str:
    """
    소스·타겟 테이블 간 ETL 검증 SQL 쿼리 6종을 자동 생성합니다.
    source_table: 소스(원본) 테이블명
    target_table: 타겟(적재 대상) 테이블명
    db_type: oracle | mariadb | postgresql (기본값 oracle)
    """
    try:
        from db_schema import get_schema
        from etl_metadata_parser import schema_to_metadata
        from etl_sql_generator import generate_validation_queries_no_llm

        schema = get_schema("db_config.json", force_refresh=False)

        src_meta = schema_to_metadata(schema, source_table)
        tgt_meta = schema_to_metadata(schema, target_table)

        queries = generate_validation_queries_no_llm(
            source_meta=src_meta,
            target_meta=tgt_meta,
            db_type=db_type,
        )

        output = []
        for key, info in queries.items():
            output.append(f"=== {info['description']} ===")
            output.append(info["sql"])
            output.append("")
        return "\n".join(output)
    except Exception as e:
        return f"검증 쿼리 생성 오류: {e}"


@tool
def suggest_rules_tool(
    source_table: str,
    target_table: str = "",
    db_type: str = "oracle",
) -> str:
    """
    데이터 프로파일을 기반으로 ETL 검증 규칙을 자동 제안합니다.
    source_table: 소스 테이블명
    target_table: 타겟 테이블명 (없으면 소스 기반만 생성)
    db_type: oracle | mariadb | postgresql
    """
    try:
        from etl_sql_generator import suggest_validation_rules

        # 소스 프로파일 — 메타데이터 우선, fallback 라이브
        src_profile = None
        try:
            from aetl_metadata_engine import get_profile_from_meta
            src_profile = get_profile_from_meta(source_table)
        except Exception:
            pass
        if not src_profile:
            from aetl_profiler import profile_table_from_config
            src_profile = profile_table_from_config("db_config.json", source_table, top_n=5)

        # 타겟 프로파일 — 메타데이터 우선, fallback 라이브
        tgt_profile = None
        if target_table.strip():
            try:
                from aetl_metadata_engine import get_profile_from_meta
                tgt_profile = get_profile_from_meta(target_table)
            except Exception:
                pass
            if not tgt_profile:
                try:
                    from aetl_profiler import profile_table_from_config
                    tgt_profile = profile_table_from_config("db_config.json", target_table, top_n=5)
                except Exception:
                    pass

        rules = suggest_validation_rules(src_profile, tgt_profile, db_type)

        if not rules:
            return "자동 제안 규칙이 없습니다. 데이터 프로파일 결과를 확인해 주세요."

        lines = [f"자동 제안된 검증 규칙 {len(rules)}건:\n"]
        for i, r in enumerate(rules, 1):
            tier_label = {1: "기술검증", 2: "정합성", 3: "비즈니스"}.get(r["tier"], "?")
            lines.append(
                f"{i}. [{tier_label}] {r['rule_name']} ({r['severity']})\n"
                f"   근거: {r['reason']}\n"
                f"   SQL:\n{r['sql']}\n"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"규칙 제안 오류: {e}"


@tool
def compare_row_counts(source_table: str, target_table: str) -> str:
    """
    소스와 타겟 테이블의 행 수를 직접 DB에서 조회하여 비교합니다.
    source_table: 소스 테이블명
    target_table: 타겟 테이블명
    """
    try:
        from db_schema import load_config
        config = load_config("db_config.json")
        db_type = config.get("db_type", "oracle").lower()
        conn_cfg = config["connection"]

        def get_count(cursor, tbl: str, is_oracle: bool) -> int:
            q = f'SELECT COUNT(*) FROM "{tbl}"' if is_oracle else f"SELECT COUNT(*) FROM `{tbl}`"
            cursor.execute(q)
            return cursor.fetchone()[0]

        if db_type == "oracle":
            import oracledb
            dsn = f"{conn_cfg['host']}:{conn_cfg['port']}/{conn_cfg['database']}"
            conn = oracledb.connect(user=conn_cfg["user"], password=conn_cfg["password"], dsn=dsn)
            cur = conn.cursor()
            src_cnt = get_count(cur, source_table, True)
            tgt_cnt = get_count(cur, target_table, True)
            conn.close()
        else:
            import mariadb
            conn = mariadb.connect(
                host=conn_cfg["host"], port=int(conn_cfg.get("port", 3306)),
                user=conn_cfg["user"], password=conn_cfg["password"],
                database=conn_cfg["database"],
            )
            cur = conn.cursor()
            src_cnt = get_count(cur, source_table, False)
            tgt_cnt = get_count(cur, target_table, False)
            conn.close()

        diff = src_cnt - tgt_cnt
        status = "PASS" if diff == 0 else "FAIL"
        return (
            f"건수 비교 결과 [{status}]\n"
            f"  소스 ({source_table}): {src_cnt:,} 건\n"
            f"  타겟 ({target_table}): {tgt_cnt:,} 건\n"
            f"  차이: {abs(diff):,} 건 {'(일치)' if diff == 0 else '(불일치!)'}"
        )
    except Exception as e:
        return f"건수 비교 오류: {e}"


@tool
def sync_metadata_tool(tables: str = "") -> str:
    """
    스키마와 프로파일 메타데이터를 SQLite에 동기화합니다.
    tables: 쉼표로 구분된 테이블명 (비워두면 전체 스키마 동기화)
    사용 예: sync_metadata_tool("") 또는 sync_metadata_tool("EMPLOYEE,DEPARTMENT")
    """
    try:
        from aetl_metadata_engine import sync_schema, sync_profile
        tbl_list = [t.strip() for t in tables.split(",") if t.strip()] or None
        schema_res = sync_schema(tables=tbl_list)
        profile_res = sync_profile(tables=tbl_list)
        synced_s = len(schema_res.get("synced", []))
        synced_p = len(profile_res.get("synced", []))
        skipped_p = len(profile_res.get("skipped", []))
        errors = schema_res.get("error", []) + profile_res.get("error", [])
        msg = (
            f"메타데이터 동기화 완료\n"
            f"  스키마: {synced_s}개 테이블 저장\n"
            f"  프로파일: {synced_p}개 테이블 수집 ({skipped_p}개 TTL 내 스킵)"
        )
        if errors:
            msg += f"\n  오류: {'; '.join(errors[:3])}"
        return msg
    except Exception as e:
        return f"메타데이터 동기화 오류: {e}"


# ─────────────────────────────────────────────────────────────
# LLM 초기화
# ─────────────────────────────────────────────────────────────
_TOOLS = [
    get_table_schema,
    search_tables,
    profile_table_tool,
    generate_validation_queries_tool,
    suggest_rules_tool,
    compare_row_counts,
    sync_metadata_tool,
]

_TOOL_MAP = {t.name: t for t in _TOOLS}

_SYSTEM_PROMPT = """당신은 AETL Agent입니다. AI 기반 ETL 어시스턴트로서 데이터베이스 메타데이터 분석,
ETL 검증 쿼리 생성, 데이터 품질 규칙 제안 등의 작업을 수행합니다.

## 사용 가능한 도구
- get_table_schema: 테이블 컬럼/타입/PK 정보 조회 (메타데이터 우선, fallback DB)
- search_tables: 키워드로 테이블 검색 (메타데이터 우선, fallback DB)
- profile_table_tool: 테이블 데이터 통계 프로파일링 (메타데이터 우선, fallback 라이브)
- generate_validation_queries_tool: 소스→타겟 검증 SQL 6종 자동 생성
- suggest_rules_tool: 프로파일 기반 검증 규칙 자동 제안 (메타데이터 우선)
- compare_row_counts: 소스·타겟 건수 직접 비교 (항상 라이브 DB)
- sync_metadata_tool: 스키마·프로파일 메타데이터를 SQLite에 사전 수집

## 행동 규칙
1. 테이블명이 명확하지 않으면 search_tables로 먼저 확인하세요.
2. 검증 쿼리 생성 전 반드시 테이블 스키마를 확인하세요.
3. 규칙 제안 시 profile_table_tool을 먼저 호출하여 데이터 특성을 파악하세요.
4. profile_table_tool이 "메타데이터 없음" 오류를 반환하면, sync_metadata_tool을 먼저 호출하거나 사용자에게 동기화를 안내하세요.
5. 응답은 한국어로, 결과를 표나 코드블록으로 명확하게 정리하세요.
6. DML(INSERT/UPDATE/DELETE/DROP)은 절대 실행하지 마세요.
"""


def _get_llm_with_tools():
    """Tool binding된 LLM 반환"""
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if google_api_key:
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            llm = ChatGoogleGenerativeAI(
                model="gemini-2.5-flash",
                google_api_key=google_api_key,
                temperature=0.0,
            )
            return llm.bind_tools(_TOOLS)
        except Exception:
            pass

    anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
    if anthropic_api_key:
        from langchain_anthropic import ChatAnthropic
        llm = ChatAnthropic(model="claude-sonnet-4-6", temperature=0.0, api_key=anthropic_api_key)
        return llm.bind_tools(_TOOLS)

    raise RuntimeError("API 키를 찾을 수 없습니다. .env 파일에 GOOGLE_API_KEY 또는 ANTHROPIC_API_KEY를 설정하세요.")


# ─────────────────────────────────────────────────────────────
# LangGraph 노드
# ─────────────────────────────────────────────────────────────

def agent_node(state: AETLState) -> dict:
    """LLM이 도구 호출 여부를 결정하는 노드"""
    llm_with_tools = _get_llm_with_tools()

    messages = list(state["messages"])
    # System prompt 삽입 (첫 번째가 아닌 경우)
    if not messages or not isinstance(messages[0], SystemMessage):
        messages = [SystemMessage(content=_SYSTEM_PROMPT)] + messages

    response = llm_with_tools.invoke(messages)
    return {"messages": [response]}


def tool_node(state: AETLState) -> dict:
    """Tool 실행 노드 — LLM이 요청한 도구를 실행"""
    last_message = state["messages"][-1]
    tool_messages = []

    for tool_call in last_message.tool_calls:
        tool_name = tool_call["name"]
        tool_args  = tool_call["args"]
        tool_id    = tool_call["id"]

        tool_fn = _TOOL_MAP.get(tool_name)
        if tool_fn:
            try:
                result = tool_fn.invoke(tool_args)
            except Exception as e:
                result = f"도구 실행 오류 ({tool_name}): {e}"
        else:
            result = f"알 수 없는 도구: {tool_name}"

        tool_messages.append(
            ToolMessage(content=str(result), tool_call_id=tool_id)
        )

    return {"messages": tool_messages}


def _should_continue(state: AETLState) -> str:
    """Tool 호출 여부에 따라 다음 노드 결정"""
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and getattr(last, "tool_calls", None):
        return "tools"
    return END


# ─────────────────────────────────────────────────────────────
# 그래프 조립
# ─────────────────────────────────────────────────────────────

def build_graph() -> Any:
    graph = StateGraph(AETLState)
    graph.add_node("agent",  agent_node)
    graph.add_node("tools",  tool_node)
    graph.set_entry_point("agent")
    graph.add_conditional_edges("agent", _should_continue, {"tools": "tools", END: END})
    graph.add_edge("tools", "agent")
    return graph.compile()


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

def run_agent(
    user_message: str,
    db_type: str = "oracle",
    config_path: str = "db_config.json",
    chat_history: list[dict] | None = None,
) -> tuple[str, list[dict]]:
    """
    에이전트를 실행합니다.

    Args:
        user_message : 사용자 자연어 요청
        db_type      : oracle | mariadb | postgresql
        config_path  : db_config.json 경로
        chat_history : 이전 대화 이력 [{"role": "user"|"assistant", "content": str}]

    Returns:
        (final_answer: str, updated_history: list[dict])
    """
    graph = build_graph()

    # 이전 이력 → BaseMessage 변환
    messages: list[BaseMessage] = []
    for msg in (chat_history or []):
        if msg["role"] == "user":
            messages.append(HumanMessage(content=msg["content"]))
        else:
            messages.append(AIMessage(content=msg["content"]))
    messages.append(HumanMessage(content=user_message))

    initial_state: AETLState = {
        "messages":    messages,
        "db_type":     db_type,
        "config_path": config_path,
    }

    final_state = graph.invoke(initial_state, config={"recursion_limit": 20})

    # 마지막 AI 응답 추출
    final_answer = ""
    for msg in reversed(final_state["messages"]):
        if isinstance(msg, AIMessage) and msg.content:
            final_answer = msg.content
            break

    # 이력 업데이트
    updated_history = list(chat_history or [])
    updated_history.append({"role": "user",      "content": user_message})
    updated_history.append({"role": "assistant",  "content": final_answer})

    return final_answer, updated_history


# ─────────────────────────────────────────────────────────────
# CLI 테스트
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_queries = [
        "사용 가능한 테이블 목록에서 'BRC' 키워드로 검색해줘",
    ]
    history: list[dict] = []
    for q in test_queries:
        print(f"\n[User] {q}")
        answer, history = run_agent(q, db_type="mariadb", chat_history=history)
        print(f"[Agent] {answer}")
