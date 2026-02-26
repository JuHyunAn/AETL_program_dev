"""
================================================================================
                    Oracle SQL 생성 챗봇 - Streamlit UI
================================================================================
[작성자] 안주현
[최종 수정] 2026-01-20
================================================================================
"""

# 라이브러리 임포트
from dotenv import load_dotenv
load_dotenv()

import re
import streamlit as st
from app import run, TokenLimitError


# =============================================================================
# SQL 포맷팅 함수
# =============================================================================
def format_sql(sql: str) -> str:
    """
    Args:
        sql (str): 포맷팅할 SQL 문자열
    Returns:
        str: 포맷팅된 SQL 문자열
    """
    if not sql:
        return sql

    # 먼저 기존 줄바꿈 정리 (여러 줄바꿈을 하나로)
    sql = re.sub(r'\s+', ' ', sql).strip()

    # 주요 키워드 앞에 줄바꿈 추가
    keywords = [
        'SELECT', 'FROM', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'JOIN',
        'WHERE', 'AND', 'OR', 'GROUP BY', 'HAVING', 'ORDER BY',
        'FETCH FIRST', 'LIMIT', 'OFFSET'
    ]

    for kw in keywords:
        # 키워드 앞에 줄바꿈 추가 (대소문자 무관)
        pattern = rf'(?i)\s+({kw})\b'
        sql = re.sub(pattern, rf'\n\1', sql)

    # SELECT 다음 컬럼들 들여쓰기
    sql = re.sub(r'(?i)^SELECT\s+', 'SELECT\n    ', sql)

    # 콤마 후 줄바꿈 (SELECT 절의 컬럼 구분)
    # SELECT와 FROM 사이의 콤마만 처리
    lines = sql.split('\n')
    formatted_lines = []
    in_select = False

    for line in lines:
        if line.strip().upper().startswith('SELECT'):
            in_select = True
        elif line.strip().upper().startswith('FROM'):
            in_select = False

        if in_select and ',' in line and not line.strip().upper().startswith('SELECT'):
            # 콤마로 분리하고 각각 들여쓰기
            parts = line.split(',')
            for i, part in enumerate(parts):
                part = part.strip()
                if part:
                    if i < len(parts) - 1:
                        formatted_lines.append(f"    {part},")
                    else:
                        formatted_lines.append(f"    {part}")
        else:
            formatted_lines.append(line)

    return '\n'.join(formatted_lines)


# =============================================================================
# 페이지 설정
# =============================================================================
st.set_page_config(page_title="Oracle SQL Chatbot", layout="wide")
st.title("Oracle SQL 생성 챗봇 (Gemini + LangGraph)")


# =============================================================================
# 세션 상태 초기화
# =============================================================================
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "memory" not in st.session_state:
    st.session_state.memory = {}

if "pending_clarification" not in st.session_state:
    st.session_state.pending_clarification = False


# =============================================================================
# 기존 대화 기록 출력
# =============================================================================
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])


# =============================================================================
# 사용자 입력 처리
# =============================================================================
user_input = st.chat_input("질문을 입력하세요. 예: 인원이 가장 많은 부서 알려줘")

if user_input:
    # 사용자 메시지 기록 및 표시
    st.session_state.chat_history.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # SQL 생성 실행
    try:
        out = run(
            question=user_input,
            chat_history=st.session_state.chat_history,
            memory=st.session_state.memory
        )
    except TokenLimitError as e:
        # 토큰 한도 초과 에러 처리
        error_msg = e.get_detail_message()
        st.session_state.chat_history.append({"role": "assistant", "content": "⚠️ API 오류 발생"})
        with st.chat_message("assistant"):
            st.error("⚠️ 토큰 한도 초과")
            st.warning(error_msg)
        st.stop()
    except Exception as e:
        # 기타 에러 처리
        st.session_state.chat_history.append({"role": "assistant", "content": f"오류: {str(e)}"})
        with st.chat_message("assistant"):
            st.error(f"오류가 발생했습니다: {str(e)}")
        st.stop()

    # 메모리 업데이트
    if out.get("memory"):
        st.session_state.memory = out["memory"]

    # 응답 처리
    if out.get("need_clarification"):
        # 추가 질문 필요
        assistant_text = f"추가 확인이 필요합니다:\n\n- {out.get('clarification_question')}"
        st.session_state.chat_history.append({"role": "assistant", "content": assistant_text})
        with st.chat_message("assistant"):
            st.markdown(assistant_text)
    else:
        # SQL 생성 완료
        sql = out["sql"]["query"]
        binds = out["sql"].get("binds", [])
        valid = out["validation"]

        # SQL 포맷팅 적용
        formatted_sql = format_sql(sql)

        assistant_text = "생성된 SQL과 바인드 변수입니다."
        st.session_state.chat_history.append({"role": "assistant", "content": assistant_text})

        with st.chat_message("assistant"):
            st.markdown(assistant_text)

            st.subheader("SQL")
            st.code(formatted_sql, language="sql")

            # st.subheader("Binds")
            # st.json(binds)

            st.subheader("Validation")
            st.json(valid)

            # 토큰 사용량 표시
            token_usage = out.get("token_usage", {})
            if token_usage.get("total_tokens", 0) > 0:
                st.subheader("Token Usage")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Input", f"{token_usage.get('input_tokens', 0):,}")
                col2.metric("Output", f"{token_usage.get('output_tokens', 0):,}")
                col3.metric("Total", f"{token_usage.get('total_tokens', 0):,}")
                col4.metric("LLM Calls", token_usage.get('llm_calls', 0))
